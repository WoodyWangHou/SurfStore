package surfstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
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
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.BlockStoreTest.BlockTestServer;
import surfstore.Utils.*;
import surfstore.Client;

public class MetadataStoreTest{
  private static final String testFolder = "../testfiles";
  private static final String testFile = testFolder + "/test.png";
  private static List<String> fileHashs;
  private static List<Block> fileBlks;
  private static ManagedChannel metadataChannel;
  private static MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;
  private static ManagedChannel blockdataChannel;
  private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;
  private static final Logger logger = Logger.getLogger(MetadataStoreTest.class.getName());
  private static MetadataTestServer testServer;
  private static BlockTestServer blockServer;
  private volatile int concurrentTestFlag = 0;

  public static class MetadataTestServer extends Thread{
      private static MetadataStore metadataStoreServer;

      private ConfigReader configr;
      // start test Block server
      public MetadataTestServer(ConfigReader config){
        super();
        configr = config;
        metadataStoreServer = new MetadataStore(config);
      }

      @Override
      public void run(){
        try{
          metadataStoreServer.buildAndRunMetaStore(this.configr);
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
    metadataStub.resetStore(clean);
    blockStub.resetStore(clean);
  }

  // Test file upload helper
  private static void upload(){
      for(Block blk : fileBlks){
        blockStub.storeBlock(blk);
      }
      return;
  }

  @BeforeAll
  public static void setUp() throws Exception {
    String configs = "../configs/configCentralized.txt";
    File configf = new File(configs);
    ConfigReader config = new ConfigReader(configf);
    fileHashs = Client.getFileHashListForTest(testFile);
    fileBlks = Client.getFileBlockListForTest(testFile);

    logger.info("============================= MetaStore Test start===============================");
    testServer = new MetadataTestServer(config);
    blockServer = new BlockStoreTest.BlockTestServer(config);
    blockServer.start();
    testServer.start();

    metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
            .usePlaintext(true).build();
    metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
    blockdataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
            .usePlaintext(true).build();
    blockStub = BlockStoreGrpc.newBlockingStub(blockdataChannel);
  }

  @AfterAll
  public static void clean(){
      testServer.interrupt();
      blockServer.interrupt();
      try{
        blockServer.join();
        testServer.join();
      }catch(Exception e){
        logger.info("Test Interrupted");
      }
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
      reset();

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
      reset();

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
      reset();

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
      reset();

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
  }

  @Test
  public void getVersionTest(){
      reset();

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
  public void deleteFileTest(){
      reset();
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
  }

  @Test
  public void concurrentTest(){
      reset();
      upload();

      for(int i = 1; i <= 10; i++){
         final FileInfo req = FileInfoUtils.toFileInfo(testFile, 1, fileHashs, false);
         int count = 0;
         Thread worker = new Thread(new Runnable(){
            @Override
            public void run(){
              WriteResult res = metadataStub.modifyFile(req);
              if(res.getResult() == WriteResult.Result.OK){
                concurrentTestFlag++;
                assertEquals(res.getResult(), WriteResult.Result.OK);
              }else{
                assertEquals(res.getResult(), WriteResult.Result.OLD_VERSION);
              }

              return;
            }
         });

         worker.start();
      }

      assertEquals(1, concurrentTestFlag);
  }
}
