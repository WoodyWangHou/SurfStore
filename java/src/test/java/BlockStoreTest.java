package surfstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.logging.Logger;
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
import surfstore.SurfStoreBasic.Block;
import surfstore.Utils.*;


public class BlockStoreTest{
  private static ManagedChannel blockChannel;
  private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;
  private static final Logger logger = Logger.getLogger(BlockStoreTest.class.getName());
  private static BlockTestServer testServer;

  public static class BlockTestServer extends Thread{
      private static BlockStore blockStoreServer;
      private ConfigReader configr;
      // start test Block server
      public BlockTestServer(ConfigReader config){
        super();
        configr = config;
        blockStoreServer = new BlockStore(config);
      }

      @Override
      public void run(){
        try{
          blockStoreServer.buildAndRunBlockStore(this.configr);
        }catch(Exception e){
          this.shutDown();
          return;
        }
      }

      public void shutDown(){
          blockStoreServer.forceStop();
      }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    String configs = "../configs/configCentralized.txt";
    File configf = new File(configs);
    ConfigReader config = new ConfigReader(configf);

    testServer = new BlockTestServer(config);
    testServer.start();
    blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
            .usePlaintext(true).build();
    blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
  }

  @AfterAll
  public static void clean(){
      testServer.interrupt();
      try{
        testServer.join();
      }catch(Exception e){
        logger.info("Test Interrupted");
      }
  }

  /**
  * Add Test Cases Below
  **/

  @Test
  @DisplayName("TEST: Ping() RPC call")
  public void pingTest(){
      Empty req = Empty.newBuilder().build();
      Empty res = blockStub.ping(req);
      assertTrue(res.equals(req));
  }

  @Test
  @DisplayName("TEST: getBlock() RPC call")
  public void getBlockTest(){
      String data = "aaa";
      String hash = HashUtils.sha256(data);
      String fake = "aab";

      Block req = BlockUtils.stringToBlock(data);
      Block fakeB = BlockUtils.stringToBlock(fake);

      blockStub.storeBlock(req);

      Block res = blockStub.getBlock(req);
      assertNotNull(res);
      assertEquals(hash, res.getHash());
      assertTrue(req.equals(res));
      assertFalse(req.equals(fakeB));
  }

  @Test
  @DisplayName("TEST: storeBlock() RPC call")
  public void storeBlockTest(){
      String data = "bbb";
      Block req = BlockUtils.stringToBlock(data);
      assertTrue(!blockStub.hasBlock(req).getAnswer());
      blockStub.storeBlock(req);
      assertTrue(blockStub.hasBlock(req).getAnswer());
      Block res = blockStub.getBlock(req);
      assertTrue(res.equals(req));
  }

  @Test
  @DisplayName("TEST: hasBlock() RPC call")
  public void hasBlockTest(){
    String data = "ccc";
    Block req = BlockUtils.stringToBlock(data);

    assertTrue(!blockStub.hasBlock(req).getAnswer());
    blockStub.storeBlock(req);
    assertTrue(blockStub.hasBlock(req).getAnswer());
  }
}
