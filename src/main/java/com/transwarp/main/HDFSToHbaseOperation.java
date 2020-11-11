package com.transwarp.main;

import com.transwarp.hyberbase.FileUtil;
import com.transwarp.hyberbase.HBaseClient;
import com.transwarp.inceptor.InceptorClient;
import org.apache.hadoop.fs.FileStatus;

import java.io.File;
import java.util.*;

public class HDFSToHbaseOperation {
    private  InceptorClient client = new InceptorClient();
    private String hbase_ip;
    private String hbase_port;
    private String table_name;
    public HDFSToHbaseOperation(Properties pps){
        hbase_ip = pps.getProperty("hyperbase_ip", "31.0.141.193");
        hbase_port = pps.getProperty("hyperbase_port", "32672");
        table_name = pps.getProperty("hyperbase_table_name", "default.voice_yunzhisheng");
    }
    public void loadHDFSFileToHbase(FileStatus file, final byte[] fileData){
        if(fileData.length == 0){
            return;
        }
        HBaseClient client = null;
        try{
            client = new HBaseClient(hbase_ip, Integer.parseInt(hbase_port));
            client.openTransport();
        }catch (Exception e) {
            e.printStackTrace();
        }
        if(client != null){
            List<Map<String, byte[]>> data = new ArrayList<Map<String, byte[]>>();
            data.add(new HashMap<String, byte[]>() {{
                put("f1:data", fileData);
            }});
            try {
                client.updateRow(table_name, file.getPath().getName(), data);
                client.closeTransport();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

//    public  void loadPicFromLocal_to_thrift(String jdbcUrl, String thrift_ip, int port, String user, String password, String  source_table_name, String target_table_name) {
////        String base_path = "C:\\Users\\jinyupeng\\Desktop\\徐家汇街道楼宇网格（专业）力量名单\\照片\\";
//        HBaseClient client = null;
//        try{
//            client = new HBaseClient(thrift_ip, port);
//            client.openTransport();
//        }catch (Exception e) {
//            e.printStackTrace();
//        }
//        File root = new File(base_path);
//        System.out.println(root.getAbsolutePath());
//        ArrayList<File> files = FileUtil.getListFiles(root);
//        for(File f: files){
//            String f_name = f.getName();
//            System.out.println(f_name);
//            String telephone = f_name.split("\\.")[0];
//            System.out.println(telephone + ">>>>>>");
//            try {
//                byte[] byte_data = FileUtil.localImage2byte(base_path + f.getName());
//                List<Map<String, byte[]>> data = new ArrayList<Map<String, byte[]>>();
//                data.add(new HashMap<String, byte[]>() {{
//                    put("f1:img", byte_data);
//                }});
//                client.updateRow(target_table_name, telephone, data);
//            }catch (Exception e) {
//                e.printStackTrace();
//                byte [] byte_data = null;
//                System.out.println("Can not find image, please check URL: " + f.getAbsolutePath());
//            }
//        }
//        if(client != null){
//            try {
//                client.closeTransport();
//            }catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//    }

}
