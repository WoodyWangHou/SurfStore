/**
* CSE291 - SurfStore - Part 1
* Course project to implement a centralized a block-based file storage service using gRPC
* @author Hou Wang, Haoting Chen
* @version 1.0
* @since 05-21-2018
*/

package surfstore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import com.google.protobuf.ByteString;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.Utils.TestUtils;
import surfstore.Utils.HashUtils;
import surfstore.Utils.BlockUtils;
import surfstore.Utils.FileInfoUtils;
import surfstore.Configs;
import com.google.common.annotations.VisibleForTesting;

public final class Client {
    // for client stub and server stub configuration
    private static final Logger logger = Logger.getLogger(Client.class.getName());
    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;
    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;
    private final ConfigReader config;

    // store file info locally
    // private HashMap<String, FileInfo> clientMetaStore;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
        // this.clientMetaStore = new HashMap<String, FileInfo>();
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
    * Above is grpc generated server code
    * Client's method for execution
    * Below is the client code to be implemented
    * Below will implement upload(FileName), download(FileName), delete(FileName), getVersion(FN)
    **/

    /**
    * upload file to blockstore with missing blocks specified
    * @param fileName file to be uploaded
    * @param missing list of blocks missing in blockstore; if missing is null, upload all file; if missing is empty, abort
    * @return void
    **/

    private void uploadMissingBlockToBlockStore(String fileName, List<String> missing){
      List<Block> blks = getFileBlockList(fileName);
      HashMap<String, Integer> miss_map = new HashMap<String, Integer>();
      // if missing is null, upload all blocks
      if(missing == null || missing.size() == 0){
        for(Block blk : blks){
          blockStub.storeBlock(blk);
          TestUtils.ensure(blockStub.hasBlock(blk).getAnswer() == true, "blockstore stored failed"); // to be commented out
        }
        return;
      }

      // if missing is not null, create map
      for(String hash : missing){
        miss_map.put(hash, 1);
      }

      for(Block blk : blks){
        String hash = blk.getHash();
        if(miss_map.containsKey(hash)){
          blockStub.storeBlock(blk);
          TestUtils.ensure(blockStub.hasBlock(blk).getAnswer() == true, "blockstore stored failed"); // to be commented out
        }
      }

      return;
    }

    /**
    * Check if file already exist in metastore.
    * @param fileName the name of fsile
    * @return current file on metastore version number
    **/
    private int getFileVersionMetaStore(String fileName){
        FileInfo req = FileInfoUtils.toFileInfo(fileName,0,null,false);
        FileInfo res = metadataStub.getVersion(req);
        return res.getVersion();
    }

    /**
    * get hash list of a given file
    * @param fileName the name of file
    * @return hash list of the file
    **/

    @VisibleForTesting
    // wrapper for testing
    public static List<String> getFileHashListForTest(String fileName){
      return getFileHashList(fileName);
    }

    private static List<String> getFileHashList(String fileName){
        List<Block> block_list = getFileBlockList(fileName);
        List<String> res = new ArrayList<String>();
        for(Block blk : block_list){
          res.add(blk.getHash());
        }

        return res;
    }

    /**
    * get block list of a given file
    * @param fileName the name of file
    * @return List<Block>
    **/

    @VisibleForTesting
    // wrapper for testing
    public static List<Block> getFileBlockListForTest(String fileName){
      return getFileBlockList(fileName);
    }

    private static List<Block> getFileBlockList(String fileName){
      Path inputPath = Paths.get(fileName);
      Path fullPath = null;
      int version = 0;
      FileChannel fc = null;
      ByteBuffer buf = null;
      List<Block> res = new ArrayList<Block>();
      try{
          fullPath = inputPath.toRealPath();
      }catch(IOException e){
        // file not exist, return empty list
          logger.warning("File not found, I/O errors");
          return null;
      }

      try{
          fc = (FileChannel)Files.newByteChannel(fullPath);
          buf = ByteBuffer.allocate(Configs.BLOCK_SIZE);
          String encoding = System.getProperty("file.encoding");

          while (fc.read(buf) > 0) {
              buf.rewind();
              byte[] data = buf.array();
              Block bl = BlockUtils.bytesToBlock(data);
              res.add(bl);
              buf.flip();
            }
        }catch(IOException e){
            logger.warning("caught IO exception: " + e.toString());
            e.printStackTrace();
            return null;
        }
      return res;
    }

    /**
    * getMissingBlocks: Query metastore to get missing blocks
    * @param fileName the name of file
    * @param version current version of the file
    * @return WriteResult check result from MetaDataStore, which contains missing blocks
    **/

    private WriteResult updateMetaStore(String fileName, int version){
        List<String> hashList = getFileHashList(fileName);
        FileInfo req = FileInfoUtils.toFileInfo(fileName, version + 1, hashList, false);
        WriteResult res = metadataStub.modifyFile(req);
        return res;
    }

    /**
    * Upload(String fileName) is the method for implementing upload() functionality in specs
    * Client needs to upload blocks to BlockStore first before signaling metastore to create
    * a file, then metastore increment file version + 1 if success, otherwise return error
    * Dependent Method: uploadToBlockStore, isFileExist, getMissingBlk
    * @param fileName file name of the file to be uploaded
    * @return boolean true if success, false if failed
    * @throws NoSuchFileException IOException RuntimeException
    **/
    private boolean upload(String fileName){
        // if local file exists and can be read, contact metastore to verify if file exists
        Path inputPath = Paths.get(fileName);
        Path fullPath = null;
        int version = 0;
        try{
            fullPath = inputPath.toRealPath();
        }catch(IOException e){
          // file not exist, return false
            logger.warning("File not found, I/O errors");
            return false;
        }

        if(!Files.isReadable(fullPath)){
            return false;
        }

        if((version = this.getFileVersionMetaStore(fileName)) > 0){
          WriteResult res = this.updateMetaStore(fileName, version);
          switch(res.getResult()){
            case OK:
              // file already exist
              logger.info("File uploaded, OK");
              return true;
            case OLD_VERSION:
              // retry upload until success
              logger.info("File version not correct,retry to upload");
              return upload(fileName);
            case MISSING_BLOCKS:
              logger.info("File missing blocks in block store, uploading blocks to block store");
              uploadMissingBlockToBlockStore(fileName, res.getMissingBlocksList());
              return upload(fileName);
            default:
            // not LEADER, change leader metastore then retry
            // TODO: to implement handling functionality
            return upload(fileName);
          }
        }else{
            logger.info("File not found on surfstore, create new file, and upload");
            uploadMissingBlockToBlockStore(fileName, null);
            WriteResult res = this.updateMetaStore(fileName, 1);
            return upload(fileName);
        }
    }

    /**
    * This is the internal method that stores server logic
    * @param void
    * @return void
    **/
  	private void go() {
          // TODO: To be commented back when implementing metadataStore
  		    // metadataStub.ping(Empty.newBuilder().build());
          // logger.info("Successfully pinged the Metadata server");

          blockStub.ping(Empty.newBuilder().build());
          logger.info("Successfully pinged the Blockstore server");

          // TODO: Implement your client here
          Block b1 = BlockUtils.stringToBlock("block_01");
          Block b2 = BlockUtils.stringToBlock("block_01");

          // TODO: can repalce ensure with jUnit Test
          TestUtils.ensure(blockStub.hasBlock(b1).getAnswer() == false);
          TestUtils.ensure(blockStub.hasBlock(b2).getAnswer() == false);

          blockStub.storeBlock(b1);
          TestUtils.ensure(blockStub.hasBlock(b1).getAnswer() == true);

          blockStub.storeBlock(b2);
          TestUtils.ensure(blockStub.hasBlock(b2).getAnswer() == true);

          Block b1prime = blockStub.getBlock(b1);
          TestUtils.ensure(b1prime.getHash().equals(b1.getHash()));
          TestUtils.ensure(b1prime.getData().equals(b1.getData()));

          logger.info("All test passed");
  	}

  	/**
  	 * TODO: Add command line handling here
  	 **/
      private static Namespace parseArgs(String[] args) {
          ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                  .description("Client for SurfStore");
          parser.addArgument("config_file").type(String.class)
                  .help("Path to configuration file");

          Namespace res = null;
          try {
              res = parser.parseArgs(args);
          } catch (ArgumentParserException e){
              parser.handleError(e);
          }
          return res;
      }

      public static void main(String[] args) throws Exception {
          Namespace c_args = parseArgs(args);
          if (c_args == null){
              throw new RuntimeException("Argument parsing failed");
          }

          File configf = new File(c_args.getString("config_file"));
          ConfigReader config = new ConfigReader(configf);

          Client client = new Client(config);

          try {
          	client.go();
          } finally {
              client.shutdown();
          }
      }
}
