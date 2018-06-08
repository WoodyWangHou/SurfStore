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

public final class Helpers{

    static final class WriteResultBuilder{
      static WriteResult toWriteResult(Result res, int cur_ver, List<String> missingHashList){
        WriteResult.Builder builder = WriteResult.newBuilder();
        builder.setResult(res)
               .setCurrentVersion(cur_ver);

        if(missingHashList == null){
            missingHashList = new ArrayList<String>();
            builder.addAllMissingBlocks((Iterable<String>) missingHashList);
        }else{
            builder.addAllMissingBlocks((Iterable<String>) missingHashList);
        }

        return builder.build();
      }
    }

    static final class FileInfoBuilder{
      static FileInfo toFileInfo(String fileName, int ver, List<String> hashList, boolean isDeleted){
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

   static final class HashBuilder{
       static String getHashCode(String s){
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

       static String getHashCode(byte[] b){
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

   static final class BlockBuilder{
       static Block hashToBlock(String h){
          Block.Builder builder = Block.newBuilder();
          try{
            builder.setHash(h)
                   .setData(ByteString.copyFrom("","UTF-8"));
          }catch(UnsupportedEncodingException e){
            throw new RuntimeException(e);
          }
          return builder.build();
        }

       static Block stringToBlock(String s){
          Block.Builder builder = Block.newBuilder();

          try{
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
          }catch (UnsupportedEncodingException e){
            throw new RuntimeException(e);
          }

          builder.setHash(HashBuilder.getHashCode(s));
          return builder.build();
        }

       static Block bytesToBlock(byte[] b){
          Block.Builder builder = Block.newBuilder();
          builder.setData(ByteString.copyFrom(b));
          builder.setHash(HashBuilder.getHashCode(b));
          return builder.build();
        }
    }
}

final class Configs{
  public static final int BLOCK_SIZE = 4096;
  public static final String UPLOAD = "upload";
  public static final String DOWNLOAD = "download";
  public static final String GETVERSION = "getversion";
  public static final String DELETE = "delete";
  public static final String NOTFOUND = "Not Found";
  public static final String OK = "OK";
}
