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
import java.util.List;
import java.util.HashMap;
import java.io.ByteArrayOutputStream;
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
  private static ManagedChannel blockdataChannel;
  private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;
  private static MetadataTestServer testServer;
  private static BlockTestServer blockServer;
  private static Client client;

  private static final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

  @BeforeAll
  public static void init() throws Exception{
      String configs = "../configs/configCentralized.txt";
      File configf = new File(configs);
      ConfigReader config = new ConfigReader(configf);

      fileHashs = Client.getFileHashListForTest(testFile);
      fileBlks = Client.getFileBlockListForTest(testFile);
      mkdir("/home/aturing/downloads");

      logger.info("============================= MetaStore Test start===============================");
      testServer = new MetadataTestServer(config);
      blockServer = new BlockStoreTest.BlockTestServer(config);
      client = new Client(config);
      blockServer.start();
      testServer.start();

      metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
              .usePlaintext(true).build();
      metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
      blockdataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
              .usePlaintext(true).build();
      blockStub = BlockStoreGrpc.newBlockingStub(blockdataChannel);

      System.setOut(new PrintStream(outContent));
  }

  @BeforeEach
  public void resetOutStream(){
    outContent.reset();
  }

  @AfterAll
  public static void clean(){
      System.setOut(System.out);
      System.setErr(System.err);
      testServer.interrupt();
      blockServer.interrupt();
      try{
        blockServer.join();
        testServer.join();
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

  /**
  * Add Test Cases Below
  **/

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
      reset();
      String arg = "../configs/configCentralized.txt getversion mytest.pdf";
      String[] args = arg.split(" ");
      Namespace c_args = Client.parseArgsForTest(args);
      client.serveForTest(c_args);

      assertEquals("0", outContent.toString());
  }

  @Test
  public void deleteNotExistFile(){
      reset();
      String arg = "../configs/configCentralized.txt delete mytest.pdf";
      String[] args = arg.split(" ");
      Namespace c_args = Client.parseArgsForTest(args);
      client.serveForTest(c_args);

      assertEquals(Configs.NOTFOUND, outContent.toString());
  }

  @Test
  public void downloadNotExistFile(){
      reset();

      String arg = "../configs/configCentralized.txt download mytest.pdf /home/aturing/downloads";
      String[] args = arg.split(" ");
      Namespace c_args = Client.parseArgsForTest(args);
      client.serveForTest(c_args);

      assertEquals(Configs.NOTFOUND, outContent.toString());
  }

  @Test
  public void uploadFile(){
      reset();

      String arg = "../configs/configCentralized.txt upload /home/aturing/mytest.pdf";
      String[] args = arg.split(" ");
      Namespace c_args = Client.parseArgsForTest(args);
      client.serveForTest(c_args);

      assertEquals(Configs.OK, outContent.toString());
  }

  // @Test
  // public void uploadFile(){
  //     reset();
  //
  //     String arg = "client ../configs/configCentralized.txt upload /home/aturing/myfile.txt";
  //     String[] args = arg.split(" ");
  //     Namespace c_args = Client.Client.parseArgsForTestForTest(args);
  //     client.serve(c_args);
  //
  //     assertEquals(Configs.OK, outContent.toString());
  // }
}
