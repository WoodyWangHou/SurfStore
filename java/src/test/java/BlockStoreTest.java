package surfstore;

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

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BlockStoreTest{
  private static ManagedChannel blockChannel;
  private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;
  private static final Logger logger = Logger.getLogger(BlockStoreTest.class.getName());
  private static BlockTestServer testServer;

  private static class BlockTestServer extends Thread{
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

  @Test
  public void UtilsTest(){
    logger.info("UtilTest completed........");
  }

  @Test
  public void storeBlockTest(){
      Block.Builder builder = Block.newBuilder();
      Block req;
      try{
        builder.setHash("aaa")
               .setData(ByteString.copyFrom("bbb", "UTF-8"));
        req = builder.build();
      }catch(UnsupportedEncodingException e){
        logger.info("not support UTF-8");
        return;
      }

      assertTrue(!blockStub.hasBlock(req).getAnswer());
      blockStub.storeBlock(req);
      assertTrue(blockStub.hasBlock(req).getAnswer());
  }
}
