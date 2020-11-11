package com.transwarp.main;

import com.transwarp.hdfs.HttpfsOperation;
import com.transwarp.hyberbase.FileUtil;
import com.transwarp.hyberbase.HBaseClient;
import com.transwarp.inceptor.InceptorClient;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import javax.security.sasl.SaslException;
import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertDataWithImg {
    private static Logger logger = LogManager.getLogger(InsertDataWithImg.class);
    private  InceptorClient client = new InceptorClient();
    private String base_path = "C:\\Users\\jinyupeng\\Desktop\\徐家汇街道楼宇网格（专业）力量名单\\更新数据-11-06\\图片\\漕河泾";
    private String guardian_access_token = "ghRCDMu8XKV5svXj6qWz-CH11409.TDH";
    private String httpfsPort = "30825";
    private String clusterIP = "31.0.141.193";
    private int hbasePort = 32672;
    public static void main(String[] args) throws Exception {
        String jdbcUrl = "jdbc:hive2://31.0.141.193:31979/xhrt_fjg;guardianToken=ghRCDMu8XKV5svXj6qWz-CH11409.TDH";
        String user = "";
        String password = "";
        String table_name = "xhrt.vw_al_sc_oss_upload";
        String clusterIP = "31.0.141.193";
        int clusterNodePort = 32672;


        InsertDataWithImg insert = new InsertDataWithImg();
        insert.start(args);

//         上传网格力量图片

        //上传历史建筑
//        insert.loadDataFromLocal_to_thrift(jdbcUrl, clusterIP, clusterNodePort, user, password,
//                "xh_fangguanju.jianzhuzhuangshi_binary",
//                "xh_fangguanju.jianzhuzhuangshi_binary", "binary_data",
//                "id", "D:\\fuangguan\\", "/data/fangguan/");



    }
    public void init(){

    }
    public void start(String[] args){

//        loadPicFromLocal_to_thrift(clusterIP,  hbasePort,
//                "grid_xh.grid_list_xh_img1");
        loadPicFromLocal_to_thrift(clusterIP, hbasePort, "C:\\Users\\jinyupeng\\Desktop\\pic\\",
                "grid_xh.grid_list_xh_img_11_06");
    }

    public String getFilePath(String base_path, HashMap oneRow, String fenge){
        StringBuilder builder = new StringBuilder(base_path);
        String firstleveltitle = (String) oneRow.get("firstleveltitle");
//            path += StringUtils.isNotEmpty(firstleveltitle) ? firstleveltitle + "\\": "";
        String secondarytitle = (String) oneRow.get("secondarytitle");
        builder.append(StringUtils.isNotEmpty(secondarytitle) ? secondarytitle + fenge: "");
        String thirdleveltitle = (String) oneRow.get("thirdleveltitle");
        builder.append(StringUtils.isNotEmpty(thirdleveltitle) ? thirdleveltitle + fenge: "");
        String fourleveltitle = (String) oneRow.get("fourleveltitle");
        builder.append(StringUtils.isNotEmpty(fourleveltitle) ? fourleveltitle + fenge: "");
        String fiveleveltitle = (String) oneRow.get("fiveleveltitle");
        builder.append(StringUtils.isNotEmpty(fiveleveltitle) ? fiveleveltitle + fenge: "");
        String sixleveltitle = (String) oneRow.get("sixleveltitle");
        builder.append(StringUtils.isNotEmpty(sixleveltitle) ? sixleveltitle + fenge: "");
        String sevenleveltitle = (String) oneRow.get("sevenleveltitle");
        builder.append(StringUtils.isNotEmpty(sevenleveltitle) ? sevenleveltitle + fenge: "");
        String eightleveltitle = (String) oneRow.get("eightleveltitle");
        builder.append(StringUtils.isNotEmpty(eightleveltitle) ? eightleveltitle + fenge: "");
        String nineleveltitle = (String) oneRow.get("nineleveltitle");
        builder.append(StringUtils.isNotEmpty(nineleveltitle) ? nineleveltitle + fenge: "");
        String tenleveltitle = (String) oneRow.get("tenleveltitle");
        builder.append(StringUtils.isNotEmpty(tenleveltitle) ? tenleveltitle + fenge: "");
        String name = oneRow.get("name").toString().replace("\uFEFF", "");
        builder.append(name);
        return builder.toString();
    }

    /***
     * 上传阿里提供的建筑数据；主要是通过获取表中路径，然后上传hyperbase
     * @param jdbcUrl
     * @param thrift_ip
     * @param port
     * @param user
     * @param password
     * @param source_table_name
     * @param target_hbase_table_name
     */
    public  void loadDataFromLocal_to_thrift(String jdbcUrl, String thrift_ip, int port, String user, String password,
                                             String  source_table_name, String target_hbase_table_name, String binary_column,
                                             String key, String local_base_path, String hdfs_base_path) throws UnsupportedEncodingException{
        ArrayList totalData = getInceptorData(jdbcUrl, user, password, source_table_name, binary_column);
        System.out.println(source_table_name +" 共有数据条数：" + totalData.size());
        if(totalData.size() == 0){
            return;
        }
        for(int i = 0; i < totalData.size(); i++){

            HashMap oneRow = (HashMap) totalData.get(i);
            String row_key = (String) oneRow.get(key);
            String name = (String) oneRow.get("name");
            String path = getFilePath(local_base_path, oneRow, "\\");
            System.out.println("正在处理文件：" + path);
            File file = new File(path);
            if(!file.exists()){
                System.out.println("文件不存在：" + path);
                continue;
            }
            if(file.length() > 10485760){
                HttpfsOperation hdfs = new HttpfsOperation();
                String put_url = "http://" + clusterIP + ":" + httpfsPort + "/webhdfs/v1" + hdfs_base_path;
                String hdfs_path = getFilePath(hdfs_base_path, oneRow, "/");
                put_url += getFilePath("", oneRow, "/");
                put_url += "?op=CREATE&data=TRUE&guardian_access_token=" + guardian_access_token;
                int status = hdfs.upload(put_url, path);
                if(status >= 200 && status < 300){
                    try {
                        saveBinaryData(thrift_ip, port, target_hbase_table_name, row_key, binary_column, hdfs_path.getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("Can not find File , please check URL: " + path);
                    }
                }
            }else {
                try {
                    byte[] byte_data = FileUtil.localImage2byte(path);
                    saveBinaryData(thrift_ip, port, target_hbase_table_name, row_key, binary_column, byte_data);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Can not find file, please check URL: " + path);
                }
            }
        }


    }

    public void saveBinaryData(String thrift_ip, int port, String target_hbase_table_name,
                               String row_key, final String binary_column, final byte[] byte_data) throws TException, SaslException{
        HBaseClient client = new HBaseClient(thrift_ip, port);
        client.openTransport();
        List<Map<String, byte[]>> data = new ArrayList<Map<String, byte[]>>();
        data.add(new HashMap<String, byte[]>() {{
            put("f2:" + binary_column, byte_data);
        }});
        client.updateRow(target_hbase_table_name, row_key, data);
        client.closeTransport();

    }

    public  void loadPicFromLocal_to_thrift(String thrift_ip, int port, String base_path, String target_table_name) {
        HBaseClient client = null;
        try{
            client = new HBaseClient(thrift_ip, port);
            client.openTransport();
        }catch (Exception e) {
            e.printStackTrace();
        }
        File root = new File(base_path);
        logger.info(root.getAbsolutePath());
        ArrayList<File> files = FileUtil.getListFiles(root);
        for(File f: files){
            String f_name = f.getName();
            logger.info(f.getAbsolutePath());
            String file_name = f_name.split("\\.")[0];
            try {
                final byte[] byte_data = FileUtil.localImage2byte(base_path + f.getName());
                List<Map<String, byte[]>> data = new ArrayList<Map<String, byte[]>>();
                data.add(new HashMap<String, byte[]>() {{
                    put("f1:img", byte_data);
                }});
                client.updateRow(target_table_name, file_name, data);
            }catch (Exception e) {
                e.printStackTrace();
                byte [] byte_data = null;
                System.out.println("Can not find image, please check URL: " + f.getAbsolutePath());
            }
        }
        if(client != null){
            try {
                client.closeTransport();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public ArrayList<HashMap> getInceptorData(String jdbcUrl, String user, String password, String  table_name,
                                              String binary_column){
        client = new InceptorClient();
        ArrayList totalData = new ArrayList();
//        String sql = "select * from " + table_name + " where thirdleveltitle = '2D005' and " + binary_column +" is null";
        String sql = "select * from " + table_name + " where "+ binary_column +" is null";
        logger.info(sql);
        try{
            totalData = client.getData(jdbcUrl, user, password, sql);

        }catch (SQLException e){
            e.printStackTrace();
        }
        return totalData;
    }

}
