package com.transwarp.main;

import com.transwarp.hyberbase.FileUtil;
import com.transwarp.hyberbase.HBaseClient;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import javax.security.sasl.SaslException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class FileInsert {
    private static Logger logger = LogManager.getLogger(FileInsert.class);
    private String hbase_ip ;
    private int hbase_port;
    private String base_path;
    private String hbase_table;
    private String hbase_file_column;
    private String guardian_access_token;
    private String httpfsPort ;


    public String getHbase_ip() {
        return hbase_ip;
    }

    public void setHbase_ip(String hbase_ip) {
        this.hbase_ip = hbase_ip;
    }

    public int getHbase_port() {
        return hbase_port;
    }

    public void setHbase_port(int hbase_port) {
        this.hbase_port = hbase_port;
    }

    public String getBase_path() {
        return base_path;
    }

    public void setBase_path(String base_path) {
        this.base_path = base_path;
    }

    public String getHbase_table() {
        return hbase_table;
    }

    public void setHbase_table(String hbase_table) {
        this.hbase_table = hbase_table;
    }

    public String getHbase_file_column() {
        return hbase_file_column;
    }

    public void setHbase_file_column(String hbase_file_column) {
        this.hbase_file_column = hbase_file_column;
    }

    public String getGuardian_access_token() {
        return guardian_access_token;
    }

    public void setGuardian_access_token(String guardian_access_token) {
        this.guardian_access_token = guardian_access_token;
    }

    public String getHttpfsPort() {
        return httpfsPort;
    }

    public void setHttpfsPort(String httpfsPort) {
        this.httpfsPort = httpfsPort;
    }

    public static void main(String[] args) {
        if(args.length != 4){
            logger.error("参数不对，参数为【配置文件路径】【文件路径】，【hbase表名】，【hbase文件列名】!");
            System.exit(1);
        }
        String base_path = args[1];
        String table_name = args[2];
        String file_column = args[3];

        FileInsert insert = new FileInsert();
        try{
            insert.init(args[0]);
        }catch (IOException e){
            e.printStackTrace();
            System.exit(1);
        }


        insert.start(base_path, table_name, file_column);
    }
    public void init(String configPath) throws IOException {
        File config_file = new File(configPath);
        if("".equals(configPath.trim()) || !config_file.exists()){
            configPath = "src/main/resources/config.properties";
        }
        Properties pps = new Properties();
        pps.load(new FileInputStream(configPath));
        setHbase_ip(pps.getProperty("hbase.thrift.ip"));
        setHbase_port(Integer.parseInt(pps.getProperty("hbase.thrift.port")));
//        setHttpfsPort(pps.getProperty("hbase.thrift.webhdfs.port"));

    }
    public void start(String base_path, String table_name, String file_column){
//        loadPicFromLocal_to_thrift(hbase_ip, hbase_port, "C:\\Users\\jinyupeng\\Desktop\\徐家汇街道楼宇网格（专业）力量名单\\更新数据-11-20\\图片\\",
//                "grid_xh.grid_list_xh_img_11_20");
        loadPicFromLocal_to_thrift(getHbase_ip(), getHbase_port(), base_path, table_name, file_column);

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

//    /***
//     * 上传阿里提供的建筑数据；主要是通过获取表中路径，然后上传hyperbase
//     * @param jdbcUrl
//     * @param thrift_ip
//     * @param port
//     * @param user
//     * @param password
//     * @param source_table_name
//     * @param target_hbase_table_name
//     */
//    public  void loadDataFromLocal_to_thrift(String jdbcUrl, String thrift_ip, int port, String user, String password,
//                                             String  source_table_name, String target_hbase_table_name, String binary_column,
//                                             String key, String local_base_path, String hdfs_base_path) throws UnsupportedEncodingException{
//        ArrayList totalData = getInceptorData(jdbcUrl, user, password, source_table_name, binary_column);
//        System.out.println(source_table_name +" 共有数据条数：" + totalData.size());
//        if(totalData.size() == 0){
//            return;
//        }
//        for(int i = 0; i < totalData.size(); i++){
//            HashMap oneRow = (HashMap) totalData.get(i);
//            String row_key = (String) oneRow.get(key);
//            String name = (String) oneRow.get("name");
//            String path = getFilePath(local_base_path, oneRow, "\\");
//            System.out.println("正在处理文件：" + path);
//            File file = new File(path);
//            if(!file.exists()){
//                System.out.println("文件不存在：" + path);
//                continue;
//            }
//            if(file.length() > 10485760){
//                HttpfsOperation hdfs = new HttpfsOperation();
////                String put_url = "http://" + clusterIP + ":" + httpfsPort + "/webhdfs/v1" + hdfs_base_path;
//                String put_url = "";
//                String hdfs_path = getFilePath(hdfs_base_path, oneRow, "/");
//                put_url += getFilePath("", oneRow, "/");
//                put_url += "?op=CREATE&data=TRUE&guardian_access_token=" + guardian_access_token;
//                int status = hdfs.upload(put_url, path);
//                if(status >= 200 && status < 300){
//                    try {
//                        saveBinaryData(thrift_ip, port, target_hbase_table_name, row_key, binary_column, hdfs_path.getBytes());
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                        System.out.println("Can not find File , please check URL: " + path);
//                    }
//                }
//            }else {
//                try {
//                    byte[] byte_data = FileUtil.localImage2byte(path);
//                    saveBinaryData(thrift_ip, port, target_hbase_table_name, row_key, binary_column, byte_data);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    System.out.println("Can not find file, please check URL: " + path);
//                }
//            }
//        }
//
//
//    }

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

    public  void loadPicFromLocal_to_thrift(String thrift_ip, int port, String base_path, String target_table_name,
                                            String column_name) {
        File root = new File(base_path);
        logger.info("文件根路径为：" + root.getAbsolutePath());
        ArrayList<File> files = FileUtil.getListFiles(root);

        HBaseClient client = null;
        try{
            client = new HBaseClient(thrift_ip, port);
            client.openTransport();
        }catch (Exception e) {
            e.printStackTrace();
        }

        for(File f: files){
            if(f.length() > 10485760){
                continue;
            }
            String file_name = f.getName();
            logger.info(f.getAbsolutePath());
            String suffix = "";
            if(file_name.contains(".")) {
                int f_index = file_name.lastIndexOf(".");
                suffix = file_name.substring(f_index + 1);
                file_name = file_name.subSequence(0, f_index).toString();
            }
            try {
                final byte[] byte_data = FileUtil.localImage2byte(base_path + f.getName());
                List<Map<String, byte[]>> data = new ArrayList<>();
                data.add(new HashMap<String, byte[]>() {{
                    put(column_name, byte_data);
                }});
                assert client != null;
                client.updateRow(target_table_name, file_name, data);
            }catch (Exception e) {
                logger.error("Can not find image, please check URL: " + f.getAbsolutePath());
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

//    public ArrayList<HashMap> getInceptorData(String jdbcUrl, String user, String password, String  table_name,
//                                              String binary_column){
//        client = new InceptorClient();
//        ArrayList totalData = new ArrayList();
////        String sql = "select * from " + table_name + " where thirdleveltitle = '2D005' and " + binary_column +" is null";
//        String sql = "select * from " + table_name + " where "+ binary_column +" is null";
//        logger.info(sql);
//        try{
//            totalData = client.getData(jdbcUrl, user, password, sql);
//
//        }catch (SQLException e){
//            e.printStackTrace();
//        }
//        return totalData;
//    }

}
