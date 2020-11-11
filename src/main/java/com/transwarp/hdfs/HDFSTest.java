package com.transwarp.hdfs;

import java.util.Properties;

public class HDFSTest {
    private static String result = "";
    public static void main(String[] args) throws Exception {
        try {
            if(args[0] != null && !"".equals(args[0]) && args.length > 0){
                HDFSOperation hdfsOperation = new HDFSOperation(new Properties());
                if("upload".equals(args[0])){
                    hdfsOperation.upload("", "");
                }else if ("delete".equals(args[0])){
                    hdfsOperation.deleteFile(args);
                }else if ("mkdir".equals(args[0])){
                    hdfsOperation.mkdirDir(args);
                }else if("list".equals(args[0])) {
//                    hdfsOperation.listDir(args);
                }else if("download".equals(args[0])){
//                    hdfsOperation.download(args);
                }else if("help".equals(args[0])){
                    System.out.println("Usage: java -jar hdfsclient-1.0-SNAPSHOT.jar [OPTION]... \n");
                    System.out.println("Provide hdfs client operations.\n");
                    System.out.println("     delete   delete file in hdfs filesystem,require filename.\n");
                    System.out.println("     mkdir    mkdir directory in filesystem,require directory name.\n");
                    System.out.println("     upload   upload require specify src filename && dst dirname.\n");
                    System.out.println("     list     show all file&directory of specify directory.\n");
                }else {
                    System.out.println("\n");
                    System.out.println("Usage: java -jar hdfsclient-1.0-SNAPSHOT.jar [OPTION]... \n");
                    System.out.println("     help     show all operations.\n");
                    System.exit(1);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
