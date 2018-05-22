/**
* CSE291 - SurfStore - Part 1
* Course project to implement a centralized a block-based file storage service using gRPC
* @author Hou Wang, Haoting Chen
* @version 1.0
* @since 05-21-2018
*/

package surfstore;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.HashMap;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.Utils.TestUtils;
import surfstore.Utils.HashUtils;
import surfstore.Utils.BlockUtils;
import surfstore.Configs;

public final class Client {
    // for client stub and server stub configuration
    private static final Logger logger = Logger.getLogger(Client.class.getName());
    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;
    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;
    private final ConfigReader config;

    // store file info locally
    private HashMap<String, FileInfo> clientMetaStore;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
        this.clientMetaStore = new HashMap<String, FileInfo>();
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
    * upload file to blockstore
    * @param fullPath absolute path of file to be uploaded
    * @param missing list of blocks missing in blockstore; if missing is null, upload all file; if missing is empty, abort
    * @return true if upload success, false otherwise
    **/

    private boolean uploadToBlockStore(Path fullPath, List<Block> missing){
        try{
            FileChannel fc = (FileChannel)Files.newByteChannel(fullPath);
            ByteBuffer buf = ByteBuffer.allocate(Configs.BLOCK_SIZE);
            String encoding = System.getProperty("file.encoding");
          }catch(IOException e){
              logger.warning("caught IO exception: " + e.toString());
              e.printStackTrace();
              return false;
          }

          while (fc.read(buf) > 0) {
              buf.rewind();
              // logger.info(Charset.forName(encoding).decode(buf));
              byte[] data = buf.array();

              Block bl = BlockUtils.bytesToBlock(data);
              // convert 4096 bytes read into block and compute Hash, store into local block store
              // clientBlockStore.put(bl.getHash(), bl.getData());

              // check if block store has the block, if not upload the block
              if(blockStub.hasBlock(bl).getAnswer() == false){
                  blockStub.storeBlock(bl);
                  ensure(blockStub.hasBlock(bl).getAnswer() == true, "Block store failed");
              }
              buf.flip();
            }

            // Update Metastore when all blocks are available in block store
    }

    /**
    * Check if file already exist in metastore.
    * @param fileName the name of file
    * @return true if file exist, false if file not created or has been deleted
    **/

    private boolean isFileExistInMetaStore(String fileName){
        FileInfo.builder builder = FileInfo.newBuilder();
        builder.setFilename(fileName);
        FileInfo req = builder.build();
        FileInfo res = metadataStub.getVersion();
        if(res.getVersion() > 0){
            return (res.getDeleted()) ? false : true;
        }else{
          return false;
        }
    }

    private List<String> getFileHashList(String fileName){
        List<Block> block_list = this.getFileBlockList(fileName);
        List<String> res = new ArrayList<String>();
        for(Block blk : block_list){
          res.add(blk.getHash());
        }

        return res;
    }

    private List<Block> getFileBlockList(String fileName){

    }

    /**
    * Query metastore to get missing blocks
    * @param fileName the name of file
    * @return List<String> hash list of missing blocks
    **/

    private List<String> getMissingBlocks(String fileName){
        FileInfo.buidler builder = FileInfo.newBuilder();
        FileInfo fileMeta = clientMetaStore.getOrDefault(fileName, null);
        int version = (fileMeta == null) ? 1 : fileMeta.getVersion() + 1;
        List<String> hashList = this.getFileHashList(fileName);

        builder.setFilename(fileName)
               .setVersion(version)
               .setBlocklist(hashList);
        FileInfo req = builder.build();
        WriteResult res = metadataStub.ModifyFile(req);

        // if write successful, need to update version
        fileMeta.setVersion(res.getCurrentVersion());
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
        // verify if the given fileName is valid and can be found locally
        Path inputPath = Path.get(fileName);
        Path fullPath = new Path();
        try{
            fullPath = inputPath.toRealPath();
        }catch(IOException e){
            logger.warning("File not found, I/O errors");
            e.printStackTrace();
            return false;
        }

        if(!Files.isReadable(fullPath)){
            throw new RuntimeException("File does not exist or not permitted to read");
        }

        // if local file exists and can be read, contact metastore to verify if file exists
        if(this.isFileExistInMetaStore(fileName)){
          List<Block> missing_blks = this.getMissingBlocks(fileName);
          return uploadToBlockStore(fullPath, missing_blks);
        }else{
          return uploadToBlockStore(fullPath, null);
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
