package surfstore;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import com.google.protobuf.ByteString;
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;

import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.WriteResult.Result;

public final class Utils{
    /**
    * WriteResult utility class
    **/
    public static final class WriteResultUtils{
      public static WriteResult toWriteResult(Result res, int cur_ver, List<String> missingHashList){
        WriteResult.Builder builder = WriteResult.newBuilder();
        builder.setResult(res)
               .setCurrentVersion(cur_ver)
               .addAllMissingBlocks((Iterable<String>) missingHashList);
        return builder.build();
      }
    }

    /**
    * FileInfo utility class
    **/
    public static final class FileInfoUtils{
      public static FileInfo toFileInfo(String fileName, int ver, List<String> hashList, boolean isDeleted){
        FileInfo.Builder builder = FileInfo.newBuilder();
        builder.setFilename(fileName)
               .setVersion(ver)
               .setDeleted(isDeleted);

        if(hashList == null){
          List<String> temp = new ArrayList<String>();
          builder.addAllBlocklist((Iterable<String>) temp);
        }else{
          builder.addAllBlocklist((Iterable<String>) hashList);
        }

        return builder.build();
      }
    }

    /**
    * Hash utility class
    **/
    public static final class HashUtils{
        public static String sha256(String s){
            MessageDigest digest = null;
            try{
                digest = digest.getInstance("SHA-256");
            }catch(NoSuchAlgorithmException e){
                e.printStackTrace();
                System.exit(2);
            }

            byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
            String encoded = Base64.getEncoder().encodeToString(hash);

            return encoded;
        }

        public static String sha256(byte[] b){
            MessageDigest digest = null;
            try{
                digest = digest.getInstance("SHA-256");
            }catch(NoSuchAlgorithmException e){
                e.printStackTrace();
                System.exit(2);
            }

            byte[] hash = digest.digest(b);
            String encoded = Base64.getEncoder().encodeToString(hash);

            return encoded;
        }
    }

    /**
    * DataToBlock Utility class
    **/
    public static final class BlockUtils{
        public static Block hashToBlock(String h){
          Block.Builder builder = Block.newBuilder();
          try{
            builder.setHash(h)
                   .setData(ByteString.copyFrom("","UTF-8"));
          }catch(UnsupportedEncodingException e){
            throw new RuntimeException(e);
          }
          return builder.build();
        }

        public static Block stringToBlock(String s){
          Block.Builder builder = Block.newBuilder();

          try{
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
          }catch (UnsupportedEncodingException e){
            throw new RuntimeException(e);
          }

          builder.setHash(HashUtils.sha256(s));
          return builder.build();
        }

        public static Block bytesToBlock(byte[] b){
          Block.Builder builder = Block.newBuilder();
          builder.setData(ByteString.copyFrom(b));
          builder.setHash(HashUtils.sha256(b));
          return builder.build();
        }
    }

    /**********************
    * Test Utility class
    **********************/
    public static final class TestUtils{
      private static final Logger logger = Logger.getLogger(Client.class.getName());
      public static void ensure(boolean b){
        if(!b){
          logger.warning("Assertion Failed");
          throw new RuntimeException("Assertion failed");
        }
      }
      // log message if test fail
      public static void ensure(boolean b, String msg){
        if(!b){
          logger.warning("Assertion Failed");
          logger.warning(msg);
          throw new RuntimeException("Assertion failed");
        }
      }
    }

}
