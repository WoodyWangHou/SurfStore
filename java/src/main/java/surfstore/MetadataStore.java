package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.Utils.FileInfoUtils;
import surfstore.Utils.BlockUtils;
import surfstore.Utils.WriteResultUtils;
import com.google.common.annotations.VisibleForTesting;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());
    protected final ManagedChannel blockChannel;
    protected final BlockStoreGrpc.BlockStoreBlockingStub blockStub;
    protected Server server;
	  protected ConfigReader config;

    public MetadataStore(ConfigReader config) {
      this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
              .usePlaintext(true).build();
      this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
    	this.config = config;
	   }

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(blockStub))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }

        if(blockChannel != null){
            blockChannel.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

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

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        protected HashMap<String, FileInfo> metadataStore;
        protected final BlockStoreGrpc.BlockStoreBlockingStub blockStub;
        protected final Semaphore readWrite = new Semaphore(1,true);
        protected final Semaphore read = new Semaphore(1);
        protected int read_count;

        public MetadataStoreImpl(BlockStoreGrpc.BlockStoreBlockingStub stub){
            super();
            this.metadataStore = new HashMap<String, FileInfo>();
            this.read_count = 0;
            this.blockStub = stub;
        }

        @Override
        public void resetStore(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            this.metadataStore.clear();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        /**
         * Read the requested file.
         * The client only needs to supply the "filename" argument of FileInfo.
         * The server only needs to fill the "version" and "blocklist" fields.
         * If the file does not exist, "version" should be set to 0.
         * This command should return an error if it is called on a server
         * that is not the leader
         * @param fileName file name can be retrieved from request
         * @return hashlist and version
         **/
        @Override
        public void readFile(FileInfo request,final StreamObserver<FileInfo> responseObserver) {
            String fileName = request.getFilename();

            // CRTICAL SECTION: read is concurrent, write is seralized with read
                try{
                    read.acquire();
                }catch(InterruptedException e){
                  throw new RuntimeException(e);
                }
                read_count++;
                if(read_count == 1){
                    try{
                        readWrite.acquire();
                    }catch(InterruptedException e){
                      throw new RuntimeException(e);
                    }
                }
            read.release();
            FileInfo res = metadataStore.getOrDefault(fileName, null);

            if(res == null){
                // not found
                res = FileInfoUtils.toFileInfo(fileName, 0, null, false);
            }

            try{
                read.acquire();
            }catch(InterruptedException e){
              throw new RuntimeException(e);
            }
                read_count--;
                if(read_count == 0)
                    readWrite.release();
            read.release();
            // end of Critical SECTION

            responseObserver.onNext(res);
            responseObserver.onCompleted();
        }

        /**
        * get missing blocks by checking with blockstore given the list of current hashes
        * if file is null, assume file not yet stored in metastore, return provided hashlist
        * This methods has to be synchronized!
        * @param file metadata
        * @return missing hash list
        **/

        private List<String> getMissingBlocks(FileInfo file){
            // if file does not exists
            List<String> hashList = file.getBlocklistList();
            List<String> missing = new ArrayList<String>();

            // file exist
            for(String hash : hashList){
                Block req = BlockUtils.hashToBlock(hash);
                if(!blockStub.hasBlock(req).getAnswer()){
                  missing.add(hash);
                }
            }

            return missing;
        }

        /**
         * Write a file.
         * The client must specify all fields of the FileInfo message.
         * The server returns the result of the operation in the "result" field.
         *
         * The server ALWAYS sets "current_version", regardless of whether
         * the command was successful. If the write succeeded, it will be the
         * version number provided by the client. Otherwise, it is set to the
         * version number in the MetadataStore.
         *
         * If the result is MISSING_BLOCKS, "missing_blocks" contains a
         * list of blocks that are not present in the BlockStore.
         *
         * This command should return an error if it is called on a server
         * that is not the leader
         * @param fileName, HashList, version number. Provided by client
         * @return OK if modify is Successful; OLD_VERSION if version is obselete; Missing Blocks if
         * certain blocks are missing
         */

        public void modifyFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {
            String fileName = request.getFilename();
            int cli_ver = request.getVersion();
            WriteResult res = null;

            // Modification of metaStore is critical:
            // CRITICAL SECTION:
            try{
                readWrite.acquire();
            }catch(InterruptedException e){
              throw new RuntimeException(e);
            }
            FileInfo cur =  metadataStore.getOrDefault(fileName, null);
            int cur_ver = (cur == null) ? 0 : cur.getVersion();
            // check if version is correct
            if(cur_ver + 1 == cli_ver){
                // version correct
                List<String> missing = this.getMissingBlocks(request);
                if(missing == null || missing.size() == 0){
                    // no missing block
                    // update block
                    FileInfo newFile = FileInfoUtils.toFileInfo(fileName, cli_ver, request.getBlocklistList(), false);
                    metadataStore.put(fileName, newFile);
                    res = WriteResultUtils.toWriteResult(WriteResult.Result.OK, cli_ver, null);
                }else{
                    res = WriteResultUtils.toWriteResult(WriteResult.Result.MISSING_BLOCKS, cur_ver, missing);
                }

            }else{
                // version not correct
                res = WriteResultUtils.toWriteResult(WriteResult.Result.OLD_VERSION, cur_ver, null);
            }
            readWrite.release();
            // end of Critical Section
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        }

        /**
         * Delete a file.
         * This has the same semantics as ModifyFile, except that both the
         * client and server will not specify a blocklist or missing blocks.
         * As in ModifyFile, this call should return an error if the server
         * it is called on isn't the leader
         * @param fileName and version
         * @return OK if deleted; OLD_VERSION if version is not correct
         */
        public void deleteFile(FileInfo request,StreamObserver<WriteResult> responseObserver) {
            String fileName = request.getFilename();
            int cli_ver = request.getVersion();
            WriteResult res = null;

            // Critical Section
            try{
                readWrite.acquire();
            }catch(InterruptedException e){
              throw new RuntimeException(e);
            }

            FileInfo cur = metadataStore.getOrDefault(fileName, null);
            int cur_ver = (cur == null) ? 0 : cur.getVersion();

            if(cur_ver == 0){
                // file not created
                res = WriteResultUtils.toWriteResult(WriteResult.Result.OK, cur_ver, null);
            }else{
                if(cur.getVersion() + 1 == cli_ver){
                    FileInfo newFile = FileInfoUtils.toFileInfo(fileName, cli_ver, cur.getBlocklistList(), true);
                    metadataStore.put(fileName, newFile);
                    res = WriteResultUtils.toWriteResult(WriteResult.Result.OK, cli_ver, null);
                }else{
                    res = WriteResultUtils.toWriteResult(WriteResult.Result.OLD_VERSION, cur_ver, null);
                }
            }

            readWrite.release();
            // end of Critical Section
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        }

        /**
         * Returns the current committed version of the requested file
         * The argument's FileInfo only has the "filename" field defined
         * The FileInfo returns the filename and version fields only
         * This should return a result even if the follower is in a
         *   crashed state
         * @param FileName
         * @return current version number
         */
        public void getVersion(FileInfo request,StreamObserver<FileInfo> responseObserver) {
            FileInfo res = null;
            String fileName = request.getFilename();

            // Critical Section:
            try{
                read.acquire();
            }catch(InterruptedException e){
              throw new RuntimeException(e);
            }
            read_count++;
            if(read_count == 1){
                try{
                    readWrite.acquire();
                }catch(InterruptedException e){
                  throw new RuntimeException(e);
                }
            }
            read.release();

            FileInfo cur = metadataStore.getOrDefault(fileName, null);
            if(cur == null){
                res = FileInfoUtils.toFileInfo(fileName, 0, null, false);
            }else{
                res = cur;
            }

            try{
                read.acquire();
            }catch(InterruptedException e){
              throw new RuntimeException(e);
            }
                read_count--;
                if(read_count == 0)
                    readWrite.release();
            read.release();
            // end of Critical Section
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        }

        // TODO: For part 2:
        /**
         * Query whether the MetadataStore server is currently the leader.
         * This call should work even when the server is in a "crashed" state
         * @param void
         * @return true if this metastore is leader; false otherwise
         */
        public void isLeader(Empty request,StreamObserver<SimpleAnswer> responseObserver) {

        }

        /**
         * "Crash" the MetadataStore server.
         * Until Restore() is called, the server should reply to all RPCs
         * with an error (except Restore) and not send any RPCs to other servers.
         */
        public void crash(Empty request,StreamObserver<Empty> responseObserver) {

        }

        /**
         * "Restore" the MetadataStore server, allowing it to start
         * sending and responding to all RPCs once again.
         */
        public void restore(Empty request,StreamObserver<Empty> responseObserver) {

        }

        /**
         * Find out if the node is crashed or not
         * (should always work, even if the node is crashed)
         * @return true if crashed; false if not
         */
        public void isCrashed(Empty request,StreamObserver<SimpleAnswer> responseObserver) {

        }
    }

    @VisibleForTesting
    // config for centralized testing, need to modify for distributed version
    void buildAndRunMetaStore(ConfigReader config) throws IOException, InterruptedException{
      final MetadataStore server = new MetadataStore(config);
      server.start(config.getMetadataPort(1), 1);
      server.blockUntilShutdown();
    }

    @VisibleForTesting
    void forceStop(){
        if(server != null){
          server.shutdown();
        }
    }
}
