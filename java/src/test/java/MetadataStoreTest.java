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
import surfstore.BlockStoreTest.BlockTestServer;
import surfstore.Utils.*;

public class MetadataStoreTest{
  private static ManagedChannel metadataChannel;
  private static MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;
  private static final Logger logger = Logger.getLogger(MetadataStoreTest.class.getName());
  private static MetadataTestServer testServer;
  private static BlockTestServer blockServer;

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

  @BeforeAll
  public static void setUp() throws Exception {
    String configs = "../configs/configCentralized.txt";
    File configf = new File(configs);
    ConfigReader config = new ConfigReader(configf);

    logger.info("============================= MetaStore Test start===============================");
    testServer = new MetadataTestServer(config);
    blockServer = new BlockStoreTest.BlockTestServer(config);
    blockServer.start();
    testServer.start();

    metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
            .usePlaintext(true).build();
    metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
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
  @DisplayName("TEST: Ping() RPC call")
  public void pingTest(){
      Empty req = Empty.newBuilder().build();
      Empty metaRes = metadataStub.ping(req);

      assertNotNull(metaRes);
      assertTrue(metaRes.equals(req));
  }
}
