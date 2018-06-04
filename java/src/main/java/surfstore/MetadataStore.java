package surfstore;

import java.io.*;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

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
import surfstore.SurfStoreBasic.LogEntry;
import surfstore.SurfStoreBasic.HeartbeatArgs;
import surfstore.SurfStoreBasic.HeartbeatReply;
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

	private void start(int port, int numThreads, ConfigReader config) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(blockStub, config, port))
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
        //check whether is leader
//        boolean isLeader = (config.getLeaderNum() == c_args.getInt("number"));
        logger.info(String.valueOf(c_args.getInt("number")));
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"), config);
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        protected HashMap<String, FileInfo> metadataStore;
        protected final BlockStoreGrpc.BlockStoreBlockingStub blockStub;
        protected Lock lock = new ReentrantLock();
        protected boolean isLeader;
        protected boolean isCrashed;
        protected ArrayList<LogEntry> logs;
        protected int last_applied = -1;
        protected int leader_commit = -1;
        // protected LinkedList<Boolean> command_channel;
        // only leader field
        protected ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub> followerStubs;
        // index of first log entry to append on follower
        protected ArrayList<Integer> next_index;


        public MetadataStoreImpl(BlockStoreGrpc.BlockStoreBlockingStub stub, ConfigReader config, int port){
            super();
            this.metadataStore = new HashMap<String, FileInfo>();
            this.blockStub = stub;
            this.isLeader = config.getMetadataPort(config.getLeaderNum()) == port;
            this.isCrashed = false;
            this.logs = new ArrayList<LogEntry>();
            // this.command_channel = new LinkedList<>();

            if (this.isLeader) {
                this.followerStubs = new ArrayList<>();
                this.next_index = new ArrayList<>();
                //add all follower's stub to leader
                for (int i = 1; i <= config.getNumMetadataServers(); i++) {
                    if (config.getLeaderNum() == i) continue; // if is leader continue;
                    final ManagedChannel metadataChannel =
                            ManagedChannelBuilder
                                    .forAddress("127.0.0.1", config.getMetadataPort(i))
                                    .usePlaintext(true).build();
                    final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub =
                            MetadataStoreGrpc
                                    .newBlockingStub(metadataChannel);
                    this.followerStubs.add(metadataStub);
                    this.next_index.add(0);
                }

                if (followerStubs.size() > 0) {

                    Thread t = new Thread(new Runnable(){
                      @Override
                      public void run() {
                          while (true) {
                              lock.lock();
                              try {
                                  for (int i = 0; i < followerStubs.size(); i++) {
                                      final MetadataStoreGrpc.MetadataStoreBlockingStub follower = followerStubs.get(i);
                                      if (follower.isCrashed(Empty.newBuilder().build()).getAnswer()) continue;

                                      HeartbeatArgs.Builder builder = HeartbeatArgs.newBuilder();
                                      ArrayList<LogEntry> appendLogs = new ArrayList<>();
                                      for (int index = next_index.get(i); index <= logs.size() - 1; index++) {
                                          appendLogs.add(logs.get(index)); // change from i -> index
                                      }
                                      builder.addAllEntries((Iterable<LogEntry>) appendLogs);
                                      builder.setLeadercommit(leader_commit);
                                      final HeartbeatArgs request = builder.build();
                                      final int index = i;

                                      // asynchronously send heartbeats to followers
                                      Thread t = new Thread(new Runnable(){
                                          @Override
                                          public void run() {
                                              try {
                                                  HeartbeatReply response = follower.heartbeat(request); // .withDeadlineAfter(1000, TimeUnit.MILLISECONDS)
                                                  next_index.set(index, response.getNextindex());
                                              } catch (Exception e) {
                                                  logger.info("heartbeat lose");
                                                  e.printStackTrace();
                                                  //do nothing.
                                              }
                                          }
                                      });
                                      t.start();
                                  }
                              } finally {
                                  lock.unlock();
                              }

                              try{
                                Thread.sleep(500);
                              }catch(Exception e){
                                logger.info("Interrupted");
                              }
                          }
                      }
                    });
                    t.start();
                }
            }

        }



        /**
         * <pre>
         * handle heartbeat request
         * </pre>
         */
        public void heartbeat(HeartbeatArgs request, StreamObserver<HeartbeatReply> responseObserver) {
            HeartbeatReply.Builder builder = HeartbeatReply.newBuilder();
            lock.lock();
            try {
                ArrayList<LogEntry> entries = new ArrayList<>(request.getEntriesList());
                if (entries.size() > 0 && logs.size() > 0) {
                    ArrayList<LogEntry> updateLogs = new ArrayList<>();
                    for (int i = 0; i < entries.get(0).getIndex(); i++) {
                        updateLogs.add(logs.get(i));
                    }
                    logs = updateLogs;
                }
                logs.addAll(entries);
                leader_commit = request.getLeadercommit();

                for (int i = last_applied + 1; i <= leader_commit; i++) {
                    applyLogEntry(logs.get(i));
                }
                builder.setNextindex(logs.size());
                HeartbeatReply response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            } finally {
                lock.unlock();
            }

        }



        @Override
        public void resetStore(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            this.metadataStore.clear();
            this.isCrashed = false;
            this.logs = new ArrayList<LogEntry>();
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
            logger.info("Client Requst: Read File: " + fileName);
            //if not found
            FileInfo res = null;
            lock.lock();
            try {
                res = FileInfoUtils.toFileInfo(fileName, 0, null, false);
                //found
                if (metadataStore.containsKey(fileName)) {
                    res = metadataStore.get(fileName);
                }
                responseObserver.onNext(res);
                responseObserver.onCompleted();
                return;
            } finally {
                lock.unlock();
            }
            // end of Critical SECTION
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
            int new_ver = request.getVersion();
            WriteResult res = null;

            // CRITICAL SECTION:
            lock.lock();
            try {
                //step1: check whether mission block
                // Why we put in step1? because instructors'guide said:
                // The MetadataStore will not create the filename to hashlist mapping
                // if any blocks are not present in the BlockStore

                FileInfo oldFile = metadataStore.getOrDefault(request.getFilename(), null);
                int old_ver = (oldFile == null) ? 0 : oldFile.getVersion();
                if(!this.isLeader){
                  res = WriteResultUtils.toWriteResult(WriteResult.Result.NOT_LEADER, old_ver, null);
                  responseObserver.onNext(res);
                  responseObserver.onCompleted();
                  return;
                }

                if (old_ver + 1 != new_ver) {//wrong version
                    res = WriteResultUtils.toWriteResult(WriteResult.Result.OLD_VERSION, old_ver, null);
                    responseObserver.onNext(res);
                    responseObserver.onCompleted();
                    return;
                }

                // Step 2 : verify if any block is missing
                List<String> missing = this.getMissingBlocks(request);
                if (missing.size() > 0) {
                    res = WriteResultUtils.toWriteResult(WriteResult.Result.MISSING_BLOCKS, old_ver, missing);
                    responseObserver.onNext(res);
                    responseObserver.onCompleted();
                    return;
                }

                res = twoPhaseCommit("modify", request);
                responseObserver.onNext(res);
                responseObserver.onCompleted();
                return;
            } finally {
                lock.unlock();
            }

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
            WriteResult res = null;
            // Critical Section
            lock.lock();
            try{
                int new_ver = request.getVersion();

                FileInfo curFile = metadataStore.getOrDefault(request.getFilename(), null);
                int cur_ver = (curFile == null) ? 0 : curFile.getVersion();

                if(!this.isLeader){
                  res = WriteResultUtils.toWriteResult(WriteResult.Result.NOT_LEADER, cur_ver, null);
                  responseObserver.onNext(res);
                  responseObserver.onCompleted();
                  return;
                }

                if (cur_ver == 0) { // file is not exist
                    res = WriteResultUtils.toWriteResult(WriteResult.Result.OK, cur_ver, null);
                    responseObserver.onNext(res);
                    responseObserver.onCompleted();
                    return;
                }

                if(cur_ver + 1 != new_ver){ //version is not correct
                    res = WriteResultUtils.toWriteResult(WriteResult.Result.OLD_VERSION, cur_ver, null);
                    responseObserver.onNext(res);
                    responseObserver.onCompleted();
                    return;
                }

                res = twoPhaseCommit("delete", request);
                responseObserver.onNext(res);
                responseObserver.onCompleted();
                return;
            } finally {
                lock.unlock();
            }
        }


        // TODO: For part 2:
        /**
         * Query whether the MetadataStore server is currently the leader.
         * This call should work even when the server is in a "crashed" state
         * @param void
         * @return true if this metastore is leader; false otherwise
         */
        public void isLeader(Empty request,StreamObserver<SimpleAnswer> responseObserver) {
            logger.info("Check current server is leader...");
            SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();

            lock.lock();
            try{
              builder.setAnswer(isLeader);
              SimpleAnswer response = builder.build();
              responseObserver.onNext(response);
              responseObserver.onCompleted();
            }finally{
              lock.unlock();
            }
        }

        /**
         * "Crash" the MetadataStore server.
         * Until Restore() is called, the server should reply to all RPCs
         * with an error (except Restore) and not send any RPCs to other servers.
         */
        public void crash(Empty request,StreamObserver<Empty> responseObserver) {
            logger.info("Crash current server...");
            lock.lock();
            try{
              isCrashed = true;
              Empty response = Empty.newBuilder().build();
              responseObserver.onNext(response);
              responseObserver.onCompleted();
            }finally{
              lock.unlock();
            }
        }

        /**
         * "Restore" the MetadataStore server, allowing it to start
         * sending and responding to all RPCs once again.
         */
        public void restore(Empty request,StreamObserver<Empty> responseObserver) {
            logger.info("Restore current server...");
            lock.lock();
            isCrashed = false;
            Empty response = Empty.newBuilder().build();
            lock.unlock();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * Find out if the node is crashed or not
         * (should always work, even if the node is crashed)
         * @return true if crashed; false if not
         */
        public void isCrashed(Empty request,StreamObserver<SimpleAnswer> responseObserver) {
            // logger.info("Check current server is crashed...");
            SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
            lock.lock();
            builder.setAnswer(isCrashed);
            SimpleAnswer response = builder.build();
            lock.unlock();
            responseObserver.onNext(response);
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
            lock.lock();
            try {
                FileInfo curFile = metadataStore.getOrDefault(fileName, null);
                if (curFile == null) {
                    res = FileInfoUtils.toFileInfo(fileName, 0, null, false);
                } else {
                    res = curFile;
                }
                responseObserver.onNext(res);
                responseObserver.onCompleted();
                return;
            } finally {
                lock.unlock();
            }
        }

        /**
         * <pre>
         * Ask the follower to commit the new log entry
         * </pre>
         */
        public void commit(LogEntry request, StreamObserver<Empty> responseObserver) {
            logger.info("commit");
            lock.lock();
            try{
              if (!isCrashed) {
                applyLogEntry(request);
              }

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
          }finally{
            lock.unlock();
          }
        }

        /**
         * <pre>
         * Ask the follower to abort the new log entry
         * </pre>
         */
        public void abort(Empty request, StreamObserver<Empty> responseObserver) {
            logger.info("abort");
            // do nothing
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
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
            List<String> missing = new ArrayList<String>();

            for(String hash : file.getBlocklistList()){
                Block req = BlockUtils.hashToBlock(hash);
                // if file does not exists
                if(!blockStub.hasBlock(req).getAnswer()){
                  missing.add(hash);
                }
            }

            return missing;
        }

        /**
         *
         * @param
         * @return
         **/

        private WriteResult twoPhaseCommit(String command, FileInfo request){
                int index = logs.size();

                LogEntry newEntry =
                        LogEntry.newBuilder()
                                .setIndex(index)
                                .setCommand(command)
                                .setRequest(request)
                                .build();

                // vote
                int count = 0;
                for (MetadataStoreGrpc.MetadataStoreBlockingStub follower : followerStubs) {
                    try {
                        SimpleAnswer response = follower.vote(newEntry); //.withDeadlineAfter(500, TimeUnit.MILLISECONDS)
                        if (response.getAnswer()) count++;
                    } catch (Exception e) {
                        //do nothing
                    }
                }

                // always commit in this project
                for (MetadataStoreGrpc.MetadataStoreBlockingStub follower : followerStubs) {
                    follower.commit(newEntry);
                }
                logs.add(newEntry);
                leader_commit = index;
                WriteResult response = applyLogEntry(newEntry);
                return response;
        }

        /**
         *
         * @param
         * @return
         **/

        private WriteResult applyLogEntry(LogEntry entry){
            if (entry.getCommand().equals("modify")) {
                WriteResult response = handleModify(entry.getRequest());
                last_applied = entry.getIndex();
                return response;
            } else if (entry.getCommand().equals("delete")) {
                WriteResult response = handleDelete(entry.getRequest());
                last_applied = entry.getIndex();
                return response;
            } else {
                logger.info("Unknown Command :"+ entry.getCommand());
                WriteResult response = null;
                return response;
            }
        }

        /**
         *
         * @param
         * @return
         **/

        private WriteResult handleModify(FileInfo request){
            logger.info("begin modify: " + request.getFilename());
            String fileName = request.getFilename();
            int new_ver = request.getVersion();
            WriteResult res = null;

            //upload file.
            FileInfo newFile = FileInfoUtils.toFileInfo(fileName, new_ver, request.getBlocklistList(), false);
            metadataStore.put(fileName, newFile);
            res = WriteResultUtils.toWriteResult(WriteResult.Result.OK, new_ver, null);
            return res;
        }

        private WriteResult handleDelete(FileInfo request){
            logger.info("begin delete: " + request.getFilename());
            String fileName = request.getFilename();
            int new_ver = request.getVersion();
            WriteResult res = null;

            // Instructor's guide shows In SurfStore, we are going to represent
            // a deleted file as a file that has a hashlist with a single hash value of “0”
            List<String> delete_sign = new ArrayList<String>();
            delete_sign.add("0");
            FileInfo newFile = FileInfoUtils.toFileInfo(fileName, new_ver, delete_sign, true);
            metadataStore.put(fileName, newFile);
            res = WriteResultUtils.toWriteResult(WriteResult.Result.OK, new_ver, null);
            return res;
        }

        /**
         * <pre>
         * vote for new command,
         * </pre>
         */
        public void vote(LogEntry request, StreamObserver<SimpleAnswer> responseObserver) {
            if (isCrashed) {
                SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
                builder.setAnswer(false);
                SimpleAnswer response = builder.build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            lock.lock();
            try {
                if (logs.size() > 0 && logs.size() + 1 < request.getIndex()) {
                  logger.info(String.valueOf(logs.size()));
                    SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
                    builder.setAnswer(false);
                    SimpleAnswer response = builder.build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                } else {
                    SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
                    builder.setAnswer(true);
                    SimpleAnswer response = builder.build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
            } finally {
                lock.unlock();
            }

        }

    }

    @VisibleForTesting
    // config for centralized testing, need to modify for distributed version
    void buildAndRunMetaStore(int port, int threads, ConfigReader config) throws IOException, InterruptedException{
      final MetadataStore server = new MetadataStore(config);
      server.start(port, threads, config);
      server.blockUntilShutdown();
    }

    @VisibleForTesting
    void forceStop(){
        if(server != null){
          server.shutdown();
        }
    }
}
