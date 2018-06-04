package surfstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.io.UnsupportedEncodingException;
import java.lang.RuntimeException;
import java.lang.Exception;
import java.lang.Thread;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.BlockStoreTest.BlockTestServer;
import surfstore.Utils.*;
import surfstore.Client;

public class TestMetadataStorePart2{
  private static final String testFolder = "../testfiles";
  private static final String testFile = testFolder + "/test.png";
  private static List<String> fileHashs;
  private static List<Block> fileBlks;
  private static ManagedChannel metadataChannel;
  private static MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;
  private static MetadataStoreGrpc.MetadataStoreBlockingStub[] followerStub;
  private static ManagedChannel blockdataChannel;
  private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;
  private static final Logger logger = Logger.getLogger(MetadataStoreTest.class.getName());
  private static MetadataTestServer[] testServer;
  private static BlockTestServer blockServer;
  private volatile int concurrentTestFlag = 0;

  public static class MetadataTestServer extends Thread{
      private static MetadataStore metadataStoreServer;
      private int port;
      private int threads;
      private ConfigReader configr;
      // start test Block server
      public MetadataTestServer(int port, int threads, ConfigReader config){
        super();
        configr = config;
        metadataStoreServer = new MetadataStore(config);
        this.port = port;
        this.threads = threads;
      }

      @Override
      public void run(){
        try{
          metadataStoreServer.buildAndRunMetaStore(this.port, this.threads, this.configr);
        }catch(Exception e){
          this.shutDown();
          return;
        }
      }

      public void shutDown(){
          metadataStoreServer.forceStop();
      }
  }

  // Test Server reset helper
  private static void reset(){
    // reset test server for testing
    Empty clean = Empty.newBuilder().build();
    try{
      metadataStub.resetStore(clean);
      blockStub.resetStore(clean);
      for(int i = 0; i < followerStub.length; i++){
        followerStub[i].resetStore(clean);
      }
    }catch(Exception e){
      e.printStackTrace();
      reset();
    }
  }

  // Test file upload helper
  private static void upload(){
      for(Block blk : fileBlks){
        blockStub.storeBlock(blk);
      }
      return;
  }

  private static void setUpServer(ConfigReader config){
    testServer = new MetadataTestServer[config.getNumMetadataServers()];

    blockServer = new BlockStoreTest.BlockTestServer(config);
    blockServer.start();
    Integer[] ids = config.getMetadataServerIds().toArray(new Integer[0]);

    for(int i = 0; i < testServer.length; i++){
      testServer[i] = new MetadataTestServer(config.getMetadataPort(ids[i]), 10, config);
      testServer[i].start();
    }
  }

  private static void setUpStubs(ConfigReader config){
    followerStub = new MetadataStoreGrpc.MetadataStoreBlockingStub[config.getNumMetadataServers() - 1];
    Integer[] ids = config.getFollowersIds().toArray(new Integer[0]);
    for(int i = 0; i < followerStub.length; i++){
      ManagedChannel stubChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(ids[i]))
              .usePlaintext(true).build();
      followerStub[i] = MetadataStoreGrpc.newBlockingStub(stubChannel);
    }

    logger.info(String.valueOf(config.getMetadataPort(config.getLeaderNum())));
    metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
            .usePlaintext(true).build();
    metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
    blockdataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
            .usePlaintext(true).build();
    blockStub = BlockStoreGrpc.newBlockingStub(blockdataChannel);
  }

  @BeforeAll
  public static void setUp(){
    fileHashs = Client.getFileHashListForTest(testFile);
    fileBlks = Client.getFileBlockListForTest(testFile);

    String configs = "../configs/configDistributed.txt";
    File configf = new File(configs);
    try{
    ConfigReader config = new ConfigReader(configf);

    logger.info("============================= MetaStore Test Part 2 start===============================");
      setUpServer(config);
      setUpStubs(config);
    }catch(Exception e){
      e.printStackTrace();
    }
  }

  @AfterAll
  public static void clean(){
    for(int i = 0; i < testServer.length; i++){
        testServer[i].interrupt();
    }

      blockServer.interrupt();
      try{
        blockServer.join();
        for(int i = 0; i < testServer.length; i++){
            testServer[i].join();
        }
      }catch(Exception e){
        logger.info("Test Interrupted");
      }
  }

  @BeforeEach
  public void testReset(){
    reset();
  }

  /**
  * Add Test Cases Below
  **/

  @Test
  public void serverSetupTest(){
    assertNotNull(testServer);
  }

  @Test
  public void pingTest(){
      Empty req = Empty.newBuilder().build();
      Empty metaRes = metadataStub.ping(req);

      assertNotNull(metaRes);
      assertTrue(metaRes.equals(req));
  }

  @Test
  public void readNotExistFile(){
      FileInfo req = FileInfoUtils.toFileInfo(testFile, 1, null, false);
      FileInfo res = metadataStub.readFile(req);

      assertNotNull(res);
      assertTrue(res.getVersion() == 0);
      assertTrue(res.getBlocklistList().size() == 0);
      assertTrue(!res.getDeleted());
  }

  @Test
  public void createNewFileNotUploadBlocks(){
      // retest reading non existing file
      FileInfo req = FileInfoUtils.toFileInfo(testFile, 1, null, false);
      FileInfo res = metadataStub.readFile(req);

      assertNotNull(res);
      assertEquals(res.getVersion(), 0);
      assertEquals(res.getBlocklistList().size(), 0);
      assertTrue(!res.getDeleted());
      // upload new file
      req = FileInfoUtils.toFileInfo(testFile, 1, fileHashs, false);
      WriteResult writeRes = metadataStub.modifyFile(req);

      assertNotNull(writeRes);
      assertEquals(writeRes.getResult(), WriteResult.Result.MISSING_BLOCKS);
      assertEquals(writeRes.getMissingBlocksList().size(), fileBlks.size(), "Returned missing list should be empty");
      assertTrue(writeRes.getCurrentVersion() == 0, "Returned version should be 0");
  }

  @Test
  public void missingBlockTest(){
      // reset test server for testing
      String fileName = testFile;
      HashMap<String, Integer> hash = new HashMap<>();
      for(String str : fileHashs){
        hash.put(str, 1);
      }

      FileInfo req = FileInfoUtils.toFileInfo(testFile, 1, fileHashs, false);
      // Assume block store has been tested

      WriteResult res = metadataStub.modifyFile(req);
      List<String> missingHashs = res.getMissingBlocksList();
      HashMap<String, Integer> missMap = new HashMap<>();

      // test missing blocks
      for(String str : missingHashs){
        missMap.put(str, 1);
        assertTrue(hash.containsKey(str));
      }

      for(String str : fileHashs){
        assertTrue(missMap.containsKey(str));
      }
  }

  @Test
  public void createNewFileWithUploadBlocks(){
      // reset test server for testing
      String fileName = testFile;
      HashMap<String, Integer> hash = new HashMap<>();
      for(String str : fileHashs){
        hash.put(str, 1);
      }
      // up load blocks
      upload();

      // re-modify file
      FileInfo req = FileInfoUtils.toFileInfo(testFile, 1, fileHashs, false);
      WriteResult writeRes = metadataStub.modifyFile(req);

      assertNotNull(writeRes);
      assertEquals(writeRes.getResult(), WriteResult.Result.OK);
      assertEquals(writeRes.getMissingBlocksList().size(), 0, "Returned missing list should be empty");
      assertTrue(writeRes.getCurrentVersion() == 1, "Returned version should be 0");

      req = FileInfoUtils.toFileInfo(testFile, 0, null, false);
      for(int i = 0; i < followerStub.length; i++){
        FileInfo res = followerStub[i].readFile(req);
        assertEquals(1, res.getVersion());
      }
  }

  @Test
  public void deleteFileTest(){
      upload();

      // Write file to metadata
      FileInfo req = FileInfoUtils.toFileInfo(testFile, 1, fileHashs, false);
      WriteResult writeRes = metadataStub.modifyFile(req);

      // Delete File:
      req = FileInfoUtils.toFileInfo(testFile, 2, null, true);
      writeRes = metadataStub.deleteFile(req);
      assertEquals(writeRes.getResult(), WriteResult.Result.OK);
      assertEquals(writeRes.getCurrentVersion(), 2);

      req = FileInfoUtils.toFileInfo(testFile, 2, null, false);
      FileInfo res = metadataStub.getVersion(req);
      assertEquals(res.getVersion(), 2);
      assertTrue(res.getDeleted());

      // Write new file
      req = FileInfoUtils.toFileInfo(testFile, 3, fileHashs, false);
      writeRes = metadataStub.modifyFile(req);
      assertEquals(writeRes.getCurrentVersion(), 3);
      res = metadataStub.getVersion(req);
      assertTrue(!res.getDeleted());

      for(int i = 0; i < followerStub.length; i++){
        req = FileInfoUtils.toFileInfo(testFile, 0, null, false);
        res = followerStub[i].readFile(req);
        logger.info("currently i is" + String.valueOf(i) + ", with version: " + String.valueOf(res.getVersion()));
        logger.info("Number of followers is : " + String.valueOf(followerStub.length));
        assertEquals(3, res.getVersion());
        assertTrue(!res.getDeleted());
      }
  }

  @Test
  public void concurrentTest(){
      upload();
      final ArrayList<FileInfo> req = new ArrayList<FileInfo>();
      req.add(FileInfoUtils.toFileInfo(testFile, 1, fileHashs, false));
      req.add(FileInfoUtils.toFileInfo(testFile, 2, null, true));
      req.add(FileInfoUtils.toFileInfo(testFile, 3, fileHashs, false));
      req.add(FileInfoUtils.toFileInfo(testFile, 4, null, true));

      Thread[] worker = new Thread[10];
      for(int i = 0; i < 10; i++){
         int count = 0;
         final int idx = i % req.size();
         worker[i] = new Thread(new Runnable(){
            @Override
            public void run(){
              WriteResult res = null;
              if(idx == 1 || idx == 3){
                   res = metadataStub.deleteFile(req.get(idx));
              }else{
                   res = metadataStub.modifyFile(req.get(idx));
              }

              if(res.getResult() == WriteResult.Result.OK){
                concurrentTestFlag++;
                assertEquals(res.getResult(), WriteResult.Result.OK);
              }else{
                assertEquals(res.getResult(), WriteResult.Result.OLD_VERSION);
              }

              return;
            }
         });

         worker[i].start();
      }

      try{
        for(int i = 0; i < 10; i++){
          worker[i].join();
        }
      }catch(Exception e){
        logger.info(e.toString());
      }
      assertEquals(4, concurrentTestFlag);

      FileInfo request = FileInfoUtils.toFileInfo(testFile, 0, null ,false);
      FileInfo res = metadataStub.readFile(request);
      FileInfo res1 = followerStub[0].readFile(request);
      logger.info("version is : " + String.valueOf(res.getVersion()));
      assertEquals(4, res.getVersion());
      assertEquals(4, res1.getVersion());
      assertTrue(res.getDeleted());
      assertTrue(res1.getDeleted());
      assertEquals(1, res.getBlocklistList().size());
      assertEquals(1, res1.getBlocklistList().size());
      assertEquals("0",res.getBlocklistList().get(0));
      assertEquals("0",res.getBlocklistList().get(0));
  }

  /**
  * Part 2 test:
  */
  @Test
  public void leaderTest(){
      Empty req = Empty.newBuilder().build();
      SimpleAnswer res = metadataStub.isLeader(req);

      assertTrue(res.getAnswer());

      for(int i = 0; i < followerStub.length; i++){
          res = followerStub[i].isLeader(req);
          assertTrue(!res.getAnswer());
      }
  }

  @Test
  public void modifyOnFollowerTest(){
      upload();
      FileInfo req = FileInfoUtils.toFileInfo(testFile, 1, fileHashs, false);
      WriteResult res = null;
      for(int i = 0; i < followerStub.length; i++){
          res = followerStub[i].modifyFile(req);
          assertEquals(WriteResult.Result.NOT_LEADER, res.getResult());
      }

      metadataStub.modifyFile(req);

      for(int i = 0; i < followerStub.length; i++){
          res = followerStub[i].deleteFile(req);
          assertEquals(WriteResult.Result.NOT_LEADER, res.getResult());
      }
  }

  @Test
  public void crashTest(){
      Empty req = Empty.newBuilder().build();
      for(int i = 0; i < followerStub.length; i++){
        followerStub[i].crash(req);
        assertTrue(followerStub[i].isCrashed(req).getAnswer());

        followerStub[i].restore(req);
        assertTrue(!followerStub[i].isCrashed(req).getAnswer());
      }
  }

  @Test
  public void normalGetVersionTest(){
      FileInfo req = FileInfoUtils.toFileInfo(testFile, 0, null, false);
      FileInfo res = metadataStub.getVersion(req);
      assertEquals(res.getVersion(), 0);

      // upload new file
      upload();
      req = FileInfoUtils.toFileInfo(testFile, 1, fileHashs, false);
      WriteResult writeRes = metadataStub.modifyFile(req);

      req = FileInfoUtils.toFileInfo(testFile, 0, null, false);
      res = metadataStub.getVersion(req);
      assertEquals(writeRes.getResult(), WriteResult.Result.OK);
      assertEquals(res.getVersion(), 1);

      req = FileInfoUtils.toFileInfo(testFile, 2, fileHashs, false);
      writeRes = metadataStub.modifyFile(req);
      res = metadataStub.getVersion(req);
      assertEquals(writeRes.getResult(), WriteResult.Result.OK);
      assertEquals(res.getVersion(), 2);

      req = FileInfoUtils.toFileInfo(testFile, 2, fileHashs, false);
      writeRes = metadataStub.modifyFile(req);
      assertEquals(writeRes.getResult(), WriteResult.Result.OLD_VERSION);
  }

  @Test
  public void crashAndGetVersion(){
      FileInfo req = FileInfoUtils.toFileInfo(testFile, 0, null, false);
      FileInfo res = metadataStub.getVersion(req);
      assertEquals(0, res.getVersion());

      if(followerStub.length > 0){
        Empty request = Empty.newBuilder().build();
        followerStub[0].crash(request);
      }

      // upload new file
      upload();
      req = FileInfoUtils.toFileInfo(testFile, 1, fileHashs, false);
      WriteResult writeRes = metadataStub.modifyFile(req);

      req = FileInfoUtils.toFileInfo(testFile, 0, null, false);
      res = metadataStub.getVersion(req);
      assertEquals(writeRes.getResult(), WriteResult.Result.OK);
      assertEquals(1, res.getVersion());


      if(followerStub.length > 0){
        res = followerStub[0].getVersion(req);
        assertEquals(0, res.getVersion());
      }
  }

  @Test
  public void crashAndRecovery(){
      FileInfo req = FileInfoUtils.toFileInfo(testFile, 0, null, false);
      FileInfo res = metadataStub.getVersion(req);
      assertEquals(0, res.getVersion());

      if(followerStub.length > 0){
        Empty request = Empty.newBuilder().build();
        followerStub[0].crash(request);
      }

      // upload new file
      upload();
      req = FileInfoUtils.toFileInfo(testFile, 1, fileHashs, false);
      WriteResult writeRes = metadataStub.modifyFile(req);

      req = FileInfoUtils.toFileInfo(testFile, 0, null, false);
      res = metadataStub.getVersion(req);
      assertEquals(WriteResult.Result.OK, writeRes.getResult());
      assertEquals(1, res.getVersion());

      // delete
      req = FileInfoUtils.toFileInfo(testFile, 2, null, true);
      writeRes = metadataStub.deleteFile(req);
      assertEquals(WriteResult.Result.OK, writeRes.getResult());
      assertEquals(2, writeRes.getCurrentVersion());

      // upload again
      req = FileInfoUtils.toFileInfo(testFile, 3, fileHashs, false);
      writeRes = metadataStub.modifyFile(req);
      assertEquals(WriteResult.Result.OK, writeRes.getResult());
      assertEquals(3, writeRes.getCurrentVersion());

      //verify follower is left behind
      req = FileInfoUtils.toFileInfo(testFile, 0, null, false);
      res = followerStub[0].readFile(req);
      assertEquals(0, res.getVersion());

      // restore
      Empty request = Empty.newBuilder().build();
      followerStub[0].restore(request);

      try{
        Thread.sleep(5000); // wait until follower to catch up
      }catch(Exception e){
        e.printStackTrace();
      }
      req = FileInfoUtils.toFileInfo(testFile, 0, null, false);
      res = followerStub[0].readFile(req);
      assertEquals(3, res.getVersion());
      assertTrue(!res.getDeleted());
  }

}
