package surfstore;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import com.google.protobuf.ByteString;
import java.util.logging.Logger;

import surfstore.SurfStoreBasic.Block;

public final class Utils{

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
    public static final class FileInfoUtils{
        public static Block toFileInfo(String fileName, int ver, byte[] blocklist){
          Block.Builder builder = Block.newBuilder();

          try{
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
          }catch (UnsupportedEncodingException e){
            throw new RuntimeException(e);
          }

          builder.setHash(HashUtils.sha256(s));
          return builder.build();
        }
    }

    /**
    * DataToBlock Utility class
    **/
    public static final class BlockUtils{
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

          try{
            builder.setData(ByteString.copyFrom(b, "UTF-8"));
          }catch (UnsupportedEncodingException e){
            throw new RuntimeException(e);
          }

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
