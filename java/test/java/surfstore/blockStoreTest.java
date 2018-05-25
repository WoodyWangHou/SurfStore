package surfstore;

import java.io.File;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import surfstore.SurfStoreBasic.Block;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BlockStoreTest{
  private BlockStore blockStoreServer;
  private final ManagedChannel blockChannel;
  private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

  @Before
  public void setUp() throws Exception {
    File configf = new File("./configCentralized.txt");
    ConfigReader config = new ConfigReader(configf);

    blockStoreServer.buildAndRunBlockStore(config);
    this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
            .usePlaintext(true).build();
    this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
  }

  @After
  public void tearDown() {
    blockStoreServer.forceStop();
  }

  @Test
  public void storeBlockTest(){
      Block.builder builder = Block.newBuidler();
      builder.setHash("aaa");
      builder.setData("bbb")
      Block req = builder.build();

      assertTrue(!blockStub.hasBlock(req).getAnswer());
      blockStub.storeBlock(req);
      assertTrue(blockStub.hasBlock(req).getAnswer());
  }
}
