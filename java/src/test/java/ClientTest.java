package surfstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.io.ByteArrayOutputStream;
import java.lang.Thread;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.lang.Exception;

import com.google.protobuf.ByteString;
import net.sourceforge.argparse4j.inf.Namespace;

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
import surfstore.MetadataStoreTest.MetadataTestServer;
import surfstore.Utils.*;
import surfstore.Client;
import surfstore.Configs;

public class ClientTest{
  private static final Logger logger = Logger.getLogger(MetadataStoreTest.class.getName());
  private static final String testFolder = "../testfiles";
  private static final String testFile = testFolder + "/test.t";

  private static List<String> fileHashs;
  private static List<Block> fileBlks;

  private static ManagedChannel metadataChannel;
  private static MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;
  private static MetadataStoreGrpc.MetadataStoreBlockingStub[] followerStub;
  private static ManagedChannel blockdataChannel;
  private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;
  private static MetadataTestServer[] testServer;
  private static BlockTestServer blockServer;
  private static Client client;

  private static final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

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

  private static void setUpTestStubs(ConfigReader config){
      followerStub = new MetadataStoreGrpc.MetadataStoreBlockingStub[config.getNumMetadataServers() - 1];
      Integer[] ids = config.getFollowersIds().toArray(new Integer[0]);
      for(int i = 0; i < ids.length; i ++){
        ManagedChannel stubChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(ids[i]))
                .usePlaintext(true).build();
        followerStub[i] = MetadataStoreGrpc.newBlockingStub(stubChannel);
      }

      metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
              .usePlaintext(true).build();
      metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
      blockdataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
              .usePlaintext(true).build();
      blockStub = BlockStoreGrpc.newBlockingStub(blockdataChannel);
  }

  private static void setUpTestStream(){
      System.setOut(new PrintStream(outContent));
  }
  @BeforeAll
  public static void init() throws Exception{
      mkdir("/home/aturing/downloads");
      String configs = "../configs/configDistributed.txt";
      File configf = new File(configs);
      ConfigReader config = new ConfigReader(configf);
      fileHashs = Client.getFileHashListForTest(testFile);
      fileBlks = Client.getFileBlockListForTest(testFile);
      logger.info("============================= MetaStore Test start===============================");
      client = new Client(config);
      setUpServer(config);
      setUpTestStubs(config);
      setUpTestStream();
  }

  @BeforeEach
  public void resetOutStream(){
    outContent.reset();
  }

  @AfterAll
  public static void clean(){
      System.setOut(System.out);
      System.setErr(System.err);
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

  // Test Server reset helper
  private static void reset(){
    // reset test server for testing
    Empty clean = Empty.newBuilder().build();
    metadataStub.resetStore(clean);
    blockStub.resetStore(clean);

    for(int i = 0; i < followerStub.length; i++){
      followerStub[i].resetStore(clean);
    }
  }

  // Test file upload helper
  private static void upload(){
      for(Block blk : fileBlks){
        blockStub.storeBlock(blk);
      }
      return;
  }

  //make directory
  private static void mkdir(String directory){
      try{
        Files.createDirectories(Paths.get(directory));
      }catch(Exception e){
        logger.info("directory creation failed");
        e.printStackTrace();
        return;
      }
  }

  private static void callClient(String arg){
      String[] args = arg.split(" ");
      Namespace c_args = Client.parseArgsForTest(args);
      try{
        client.serveForTest(c_args);
      }catch(Exception e){
        e.printStackTrace();
      }
  }

  private static String getExpectedVersion(String ver, int servers){
      String res = "";
      for(int i = 0; i < servers; i++){
         res  += ver + " ";
      }

      return res.substring(0, res.length() - 1);
  }

  private static void sleep(){
    try{
      Thread.sleep(500);
    }catch(Exception e){
      e.printStackTrace();
    }
  }

  /**
  * Add Test Cases Below
  **/
  @BeforeEach
  public void testReset(){
    reset();
  }

  @BeforeAll
  public static void logging(){
    logger.info("================================== Client Test ========================================");
  }

  @Test void pingTest(){
      Empty req = Empty.newBuilder().build();
      Empty res = blockStub.ping(req);
      assertNotNull(res);

      Empty res2 = metadataStub.ping(req);
      assertNotNull(res);
  }

  @Test
  public void getBlockTest(){
    String fileName = testFile;
    List<String> fileHashs = Client.getFileHashListForTest(fileName);
    List<Block> fileBlocks = Client.getFileBlockListForTest(fileName);
    HashMap<String, Block> blocmap = new HashMap<>();

    for(Block blk : fileBlocks){
      blocmap.put(blk.getHash(), blk);
    }

    assertNotNull(fileHashs);
    assertNotNull(fileBlocks);
    assertEquals(fileHashs.size(),fileBlocks.size());

    for(String hash : fileHashs){
      assertTrue(blocmap.containsKey(hash));
    }
  }

  @Test
  public void getNotExistFileVersion(){
      String arg = "../configs/configDistributed.txt getversion mytest.pdf";
      callClient(arg);

      String answer = getExpectedVersion("0", testServer.length);
      assertEquals(answer, outContent.toString());
  }

  @Test
  public void deleteNotExistFile(){
      String arg = "../configs/configDistributed.txt delete mytest.pdf";
      callClient(arg);

      assertEquals(Configs.NOTFOUND, outContent.toString());
  }

  @Test
  public void downloadNotExistFile(){
      String arg = "../configs/configDistributed.txt download mytest.pdf /home/aturing/downloads";
      callClient(arg);

      assertEquals(Configs.NOTFOUND, outContent.toString());
  }

  @Test
  public void uploadFile(){
      String arg = "../configs/configDistributed.txt upload /home/aturing/mytest.pdf";
      callClient(arg);

      assertEquals(Configs.OK, outContent.toString());
      FileInfo req = FileInfoUtils.toFileInfo("mytest.pdf", 0, null, false);
      FileInfo res = metadataStub.readFile(req);
      assertEquals(1, res.getVersion());
      assertTrue(res.getBlocklistList().size() > 0);
  }

  @Test
  public void downloadExistFile(){
      String arg = "../configs/configDistributed.txt upload /home/aturing/mytest.pdf";
      callClient(arg);

      assertEquals(Configs.OK, outContent.toString());
      outContent.reset();
      arg = "../configs/configDistributed.txt download mytest.pdf /home/aturing/downloads";
      callClient(arg);

      assertEquals(Configs.OK, outContent.toString());
      assertTrue(Files.exists(Paths.get("/home/aturing/downloads/mytest.pdf")));
  }

  @Test
  public void uploadAndGetVersion(){
      String arg = "../configs/configDistributed.txt upload /home/aturing/mytest.pdf";
      callClient(arg);

      outContent.reset();
      arg = "../configs/configDistributed.txt getversion mytest.pdf";
      callClient(arg);

      sleep();
      String answer = getExpectedVersion("1", testServer.length);
      assertEquals(answer, outContent.toString());

      // Second upload
      arg = "../configs/configDistributed.txt upload /home/aturing/mytest.pdf";
      callClient(arg);

      outContent.reset();
      arg = "../configs/configDistributed.txt getversion mytest.pdf";
      callClient(arg);

      answer = getExpectedVersion("2", testServer.length);
      assertEquals(answer, outContent.toString());

      // Second upload
      outContent.reset();
      arg = "../configs/configDistributed.txt delete mytest.pdf";
      callClient(arg);
      assertEquals(Configs.OK, outContent.toString());

      outContent.reset();
      arg = "../configs/configDistributed.txt getversion mytest.pdf";
      callClient(arg);

      answer = getExpectedVersion("3", testServer.length);
      assertEquals(answer, outContent.toString());

  }

  @Test
  public void downloadDeleted(){
      String arg = "../configs/configDistributed.txt upload /home/aturing/mytest.pdf";
      callClient(arg);

      // Second upload
      arg = "../configs/configDistributed.txt delete mytest.pdf";
      callClient(arg);

      outContent.reset();
      arg = "../configs/configDistributed.txt getversion mytest.pdf";
      callClient(arg);

      sleep();
      String answer = getExpectedVersion("2", testServer.length);
      assertEquals(answer, outContent.toString());

      outContent.reset();
      arg = "../configs/configDistributed.txt download mytest.pdf /home/aturing/downloads";
      callClient(arg);

      assertEquals(Configs.NOTFOUND, outContent.toString());
  }

  @Test
  public void crashAndDownload(){
      Empty req = Empty.newBuilder().build();
      followerStub[0].crash(req);

      String arg = "../configs/configDistributed.txt upload /home/aturing/mytest.pdf";
      callClient(arg);

      // Second upload
      arg = "../configs/configDistributed.txt delete mytest.pdf";
      callClient(arg);

      outContent.reset();
      arg = "../configs/configDistributed.txt getversion mytest.pdf";
      callClient(arg);

      sleep();
      String answer = getExpectedVersion("2", testServer.length);
      assertNotEquals(answer, outContent.toString());
      followerStub[0].restore(req);

      outContent.reset();
      arg = "../configs/configDistributed.txt download mytest.pdf /home/aturing/downloads";
      callClient(arg);

      assertEquals(Configs.NOTFOUND, outContent.toString());
  }
}
