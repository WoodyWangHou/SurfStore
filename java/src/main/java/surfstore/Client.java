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
import java.nio.file.StandardOpenOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.NoSuchFileException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.Set;
import java.util.HashSet;
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
        // only connect to leader metadata
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
    * upload file to blockstore with missing blocks specified
    * @param filePath path of local file to be uploaded
    * @param missing list of blocks missing in blockstore; if missing is null, upload all file; if missing is empty, abort
    * @return void
    **/
    private void uploadMissingBlockToBlockStore(Path filePath, List<String> missing){
      List<Block> blks = getFileBlockList(filePath.toString());
      HashMap<String, Integer> miss_map = new HashMap<String, Integer>();
      // if missing is null, upload all blocks
      if(missing == null || missing.size() == 0){
        for(Block blk : blks){
          blockStub.storeBlock(blk);
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
        }
      }

      return;
    }

    /**
    * get file version from lead metadata store
    * @param fileName the name of file
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
    public static List<String> getFileHashListForTest(String localPath){
      return getFileHashList(localPath);
    }

    private static List<String> getFileHashList(String localPath){
        List<Block> block_list = getFileBlockList(localPath);
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
    public static List<Block> getFileBlockListForTest(String localPath){
      return getFileBlockList(localPath);
    }

    private static List<Block> getFileBlockList(String localPath){
      List<Block> res = new ArrayList<Block>();
      Path fullPath = Paths.get(localPath);
      if(fullPath == null){
        return res;
      }

      int version = 0;
      FileChannel fc = null;
      ByteBuffer buf = null;
      try{
          fc = (FileChannel)Files.newByteChannel(fullPath, StandardOpenOption.READ);
          buf = ByteBuffer.allocate(Configs.BLOCK_SIZE);
          String encoding = System.getProperty("file.encoding");

          while (fc.read(buf) > 0) {
              byte[] data = buf.array();
              Block bl = BlockUtils.bytesToBlock(data);
              res.add(bl);
              buf.flip();
            }
            fc.close();
        }catch(IOException e){
            logger.warning("caught IO exception: " + e.toString());
            e.printStackTrace();
            return null;
        }
      return res;
    }

    /**
    * getMissingBlocks: Query metastore to get missing blocks
    * @param localPath Path of file
    * @param version version of the file wish to upload
    * @return WriteResult check result from MetaDataStore, which contains missing blocks
    **/
    private WriteResult updateMetaStore(Path localPath, int version){
        String fileName = localPath.getFileName().toString();
        List<String> hashList = getFileHashList(localPath.toString());
        FileInfo req = FileInfoUtils.toFileInfo(fileName, version, hashList, false);
        WriteResult res = metadataStub.modifyFile(req);
        return res;
    }

    /**
    * get Path object given file path string
    * @param localPath String of local file Path
    * @return Path object; null if file cannot be found
    **/
    private Path getFilePath(String localPath){
        Path inputPath = Paths.get(localPath);
        Path fullPath = null;

        try{
            fullPath = inputPath.toRealPath();
        }catch(IOException e){
          // file not exist, return false
            logger.warning("File not found, I/O errors");
            return null;
        }

        if(!Files.isReadable(fullPath)){
            return null;
        }

        if(!Files.exists(fullPath)){
            return null;
        }

        return fullPath;
    }

    /**
    * update local file with current hash list
    * @param localFilePath the path object containing local file
    * @param missingBlocks list of blocks not current in local file
    * @param currentHashList the current file hash list
    * @return void
    */
    private void modifyLocalFileToRemote(Path localFilePath, List<Block> missingBlocks, List<String> currentHashList){
        List<Block> localBlocks = getFileBlockList(localFilePath.toString());
        HashMap<String, byte[]> blockMap = new HashMap<>();
        FileChannel fc = null;
        for(Block blk : missingBlocks){
          blockMap.put(blk.getHash(), blk.getData().toByteArray());
        }

        for(Block blk : localBlocks){
          blockMap.put(blk.getHash(), blk.getData().toByteArray());
        }

        try{
            fc = (FileChannel) Files.newByteChannel(localFilePath, StandardOpenOption.WRITE);
            String encoding = System.getProperty("file.encoding");

            for(String hash : currentHashList) {
                ByteBuffer buf = ByteBuffer.wrap(blockMap.get(hash));
                fc.write(buf);
              }
              fc.close();
          }catch(IOException e){
              logger.warning("caught IO exception: " + e.toString());
              e.printStackTrace();
              return;
          }
    }

    /**
    * create new empty file
    * @param fileName
    * @param destFolder
    * @return Path object to newly created file; null if file already exist
    */
    private Path createFile(String fileName, String destFolder){
        Path res = Paths.get(destFolder + "/" + fileName);
        HashSet<PosixFilePermission> permissiongSet = new HashSet<>();
        permissiongSet.add(PosixFilePermission.OTHERS_WRITE);
        permissiongSet.add(PosixFilePermission.OTHERS_READ);
        permissiongSet.add(PosixFilePermission.OWNER_WRITE);
        permissiongSet.add(PosixFilePermission.OWNER_READ);

        try{
            Files.createDirectories(Paths.get(destFolder), PosixFilePermissions.asFileAttribute(permissiongSet));
            res = Files.createFile(res, PosixFilePermissions.asFileAttribute(permissiongSet));
        }catch(FileAlreadyExistsException  e){
          logger.info("file already exist " + e.toString());
          e.printStackTrace();
          return null;
        }catch(IOException e){
          logger.info("permission denied");
          e.printStackTrace();
          return null;
        }

        return res;
    }

    /**
    * compare and get the hash list that local file is missing
    * @param filePath
    * @param curHashList current file hash list in metadata store
    * @return list of missing hash list
    */
    private List<String> getLocalMissingHashList(Path filePath, List<String> curHashList){
        List<String> localHashList = getFileHashList(filePath.toString());
        HashMap<String, Integer> missing_map = new HashMap<String, Integer>();
        List<String> res = new ArrayList<String>();

        for(String hash : curHashList){
            missing_map.put(hash, 1);
        }

        for(String hash : localHashList){
            if(missing_map.containsKey(hash)){
              missing_map.put(hash, 0);
            }
        }

        for(String hash : missing_map.keySet()){
            if(missing_map.get(hash) > 0){
                res.add(hash);
            }
        }

        return res;
    }

    /**
    * download blocks from block store
    * @param hashList
    * @return list of blocks downloaded
    */
    private List<Block> downloadBlocks(List<String> hashList){
        List<Block> downloadedBlocks = new ArrayList<Block>();
        for(String hash : hashList){
            Block req = BlockUtils.hashToBlock(hash);
            if(blockStub.hasBlock(req).getAnswer()){
              downloadedBlocks.add(blockStub.getBlock(req));
            }
        }
        return downloadedBlocks;
    }

    /**
    * Upload(String fileName) is the method for implementing upload() functionality in specs
    * Client needs to upload blocks to BlockStore first before signaling metastore to create
    * a file, then metastore increment file version + 1 if success, otherwise return error
    * Dependent Method: uploadToBlockStore, isFileExist, getMissingBlk
    * @param fileName remote file name of the file to be uploaded
    * @param localPath local file path
    * @return boolean true if success, false if metadataStore return NOT_LEADER
    * @throws NoSuchFileException
    **/
    protected boolean upload(String localPath) throws NoSuchFileException{
        // if local file exists and can be read, contact metastore to verify if file exists
        int version = 0;
        Path fullPath = getFilePath(localPath);
        if(fullPath == null){
            throw new NoSuchFileException("file cannot be found");
        }
        String fileName = fullPath.getFileName().toString();
        version = getFileVersionMetaStore(fileName);
        WriteResult res = updateMetaStore(fullPath, version + 1);
        switch(res.getResult()){
          case OK:
            // file already exist
            return true;
          case OLD_VERSION:
            // retry upload until success
            return upload(localPath);
          case MISSING_BLOCKS:
            uploadMissingBlockToBlockStore(fullPath, res.getMissingBlocksList());
            return upload(localPath);
          default:
          // not LEADER, change leader metastore then retry
          // return false if not leader
          return false;
        }
      }

    /**
    * getVersion(fileName) is the method for reading versions of a file from surfstore
    * it will read one version if centralized
    * return three versions if distributed
    * @param fileName
    * @return list of versions (1 if in centralized mode)
    **/
    protected List<Integer> getVersion(String fileName){
        List<Integer> result = new ArrayList<Integer>();
        FileInfo req = FileInfoUtils.toFileInfo(fileName, 0, null, false);
        // get version from primary
        result.add(getFileVersionMetaStore(fileName));

        // Get Version from followers
        for(int id: config.getFollowersIds()){
          ManagedChannel metadataFollowerChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(id))
                                                         .usePlaintext(true).build();
          MetadataStoreGrpc.MetadataStoreBlockingStub  followerStub = MetadataStoreGrpc.newBlockingStub(metadataFollowerChannel);
          FileInfo res = followerStub.getVersion(req);
          result.add(res.getVersion());
          metadataFollowerChannel.shutdown();
        }

        return result;
    }

    /**
    * delete(fileName), delete a remote file from metastore
    * Client specifies filename, verison, and delete
    * @param fileName remote file name
    * @return true if delete succeed, false if error
    * @throws NoSuchFileException
    */
    protected boolean delete(String fileName) throws NoSuchFileException{
        int version = getFileVersionMetaStore(fileName);
        if(version == 0){
          throw new NoSuchFileException("No file found in remote");
        }

        FileInfo req = FileInfoUtils.toFileInfo(fileName, version + 1, null, true);
        WriteResult res = metadataStub.deleteFile(req);
        switch(res.getResult()){
            case OK:
              return true;
            case OLD_VERSION:
              // retry delete until success
              logger.info("version is obselete, retry deletion");
              return delete(fileName);
            case NOT_LEADER:
              logger.info("NOT LEADER, delete failed");
              return false;
            default:
              return false;
        }
    }

    /**
    * download(fileName) is the method for downloading a file from surfstore
    * it will contact leader node to download if distributed
    * @param FileName remote file name
    * @param destFolder destination folder path
    * @return true if download succeed; false if download failed
    * @throws NoSuchFileException
    */
    protected void download(String fileName, String destFolder) throws NoSuchFileException{
        List<String> local_missing = null;
        String localFilePathName = destFolder + "/" + fileName;
        Path localFilePath = getFilePath(localFilePathName);
        if(localFilePath == null){
            localFilePath = createFile(fileName, destFolder);
        }

        FileInfo req = FileInfoUtils.toFileInfo(fileName, 0, null, false);
        FileInfo res = metadataStub.readFile(req);

        // file deleted
        if(res.getVersion() == 0 || res.getDeleted()){
            throw new NoSuchFileException("No such file remotely");
        }

        List<String> curHashList = res.getBlocklistList();
        local_missing = getLocalMissingHashList(localFilePath, curHashList);
        List<Block> missing_downloaded = downloadBlocks(local_missing);
        logger.info(String.valueOf(missing_downloaded.size()));
        modifyLocalFileToRemote(localFilePath, missing_downloaded, curHashList);
    }

    /**
    * Main client thread: specifying services
    */
    @VisibleForTesting
    public void serveForTest(Namespace args){
      this.serve(args);
    }

    private void serve(Namespace args){
        String cmd = args.getString("command");
        String fileName = "";
        switch(cmd){
          case Configs.UPLOAD:
            String localPath = args.getString("file_name");
            try{
                if(!this.upload(localPath)){
                  // not sure how to handle this
                  System.out.print("Failed");
                }else{
                  System.out.print(Configs.OK);
                }
            }catch(NoSuchFileException e){
                System.out.print(Configs.NOTFOUND);
            }
            break;
          case Configs.DOWNLOAD:
            fileName = args.getString("file_name");
            String destFolder = args.getString("local_folder");
            try{
                download(fileName, destFolder);
            }catch(NoSuchFileException e){
                System.out.print(Configs.NOTFOUND);
                break;
            }

            System.out.print(Configs.OK);
            break;
          case Configs.DELETE:
            fileName = args.getString("file_name");
            try{
                if(!this.delete(fileName)){
                    System.out.print("MetadataStore is not the leader");
                }else{
                    System.out.print(Configs.OK);
                }
            }catch(NoSuchFileException e){
                System.out.print(Configs.NOTFOUND);
            }
            break;
          default: //getversion
            fileName = args.getString("file_name");
            List<Integer> versions = this.getVersion(fileName);
            System.out.print(versions.get(0));

            for(int i = 1; i < versions.size(); i++){
              System.out.print(" " + versions.get(i));
            }
        }
    }
  	/**
  	 * TODO: Add command line handling here
  	 **/
     @VisibleForTesting
     public static Namespace parseArgsForTest(String[] args){
        return parseArgs(args);
     }

      private static Namespace parseArgs(String[] args) {
          ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                  .description("Client for SurfStore");
          parser.addArgument("config_file").type(String.class)
                  .help("Path to configuration file");
          parser.addArgument("command").type(String.class)
                  .help("Surfstore command: -upload, -download, -delete, -getversion");
          parser.addArgument("file_name").type(String.class)
                  .help("local or remote file name, corresponding to command");
          parser.addArgument("local_folder").nargs("?").type(String.class)
                  .help("folder for file downloads");

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
          	client.serve(c_args);
          } finally {
              client.shutdown();
          }
      }
}
