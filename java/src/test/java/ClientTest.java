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

import com.google.protobuf.ByteString;

import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.BlockStoreTest.BlockTestServer;
import surfstore.Utils.*;
import surfstore.Client;

public class ClientTest{
  private static final Logger logger = Logger.getLogger(MetadataStoreTest.class.getName());
  private static final String testFolder = "../testfiles";
  private static final String testFile = testFolder + "/test.t";
  private static ManagedChannel metadataChannel;
  private static MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;
  private static ManagedChannel blockdataChannel;
  private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;

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
  
  /**
  * Add Test Cases Below
  **/

  @BeforeAll
  public static void logging(){
    logger.info("================================== Client Test ====================");
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
}
