package com.transwarp.hdfs;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;

public class HttpfsOperation {
    public int upload(String url, String path) throws UnsupportedEncodingException {
        HttpClient client = new HttpClient();
        int status = -1;
        PutMethod method = new PutMethod(encoder(url));
        method.setRequestHeader("Content-Type","application/octet-stream");
        try {
            // 设置上传文件
            File targetFile = new File(path);
            FileInputStream in =new FileInputStream(targetFile);
            method.setRequestBody(in);
            status = client.executeMethod(method);
        } catch (Exception e) {
            e.printStackTrace();
        }
        method.releaseConnection();
        return status;
    }


    public void listDirs(String url){
        HttpClient client = new HttpClient();
        int status = -1;
        GetMethod method = new GetMethod(url);
//        method.setRequestHeader("Content-Type","application/octet-stream");
        try {
            // 设置上传文件
            status = client.executeMethod(method);
//            System.out.println(method.getResponseBodyAsString());
            System.out.println(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
        method.releaseConnection();
    }
    public void download(String url, String localPath){
        HttpClient client = new HttpClient();
        int status = -1;
        GetMethod method = new GetMethod(url);
        try {
            // 设置下载文件
            File file = new File(localPath);
            status = client.executeMethod(method);
            byte[] getData= method.getResponseBody();
            FileOutputStream fos = new FileOutputStream(file);
            fos.write(getData);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        method.releaseConnection();
    }
    public static void main(String[] args) throws UnsupportedEncodingException {

        String tmp = "房管";
//        String put_url = "http://31.0.141.194:30825/webhdfs/v1/tmp/"+ URLEncoder.encode(tmp, "utf-8") +"?op=CREATE&data=TRUE&guardian_access_token=ghRCDMu8XKV5svXj6qWz-CH11409.TDH";
        String put_url = "http://31.0.141.194:30825/webhdfs/v1/tmp/" + URLEncoder.encode(tmp) + "/tables.doc?op=CREATE&data=TRUE&guardian_access_token=ghRCDMu8XKV5svXj6qWz-CH11409.TDH";
        String get_url = "http://31.0.141.194:30825/webhdfs/v1/tmp/xujing/commons-httpclient-3.1.jar?op=open&guardian_access_token=ghRCDMu8XKV5svXj6qWz-CH11409.TDH";
        String list_url = "http://31.0.141.194:30825/webhdfs/v1/data/fangguan/%E5%BB%BA%E7%AD%91%E8%A3%85%E9%A5%B0/1D004/1D004-01-01/?op=LISTSTATUS&guardian_access_token=ghRCDMu8XKV5svXj6qWz-CH11409.TDH";
        String file_path = "D:\\fuangguan\\建筑装饰\\2D004\\2D004-57-33\\2D004-57-33照片\\2D004-57-33-北立面 (2)-20161220.JPG";
        HttpfsOperation operation = new HttpfsOperation();
        put_url = operation.encoder("http://31.0.141.194:30825/webhdfs/v1/data/fangguan/建筑装饰/2D004/2D004-57-33/2D004-57-33照片/2D004-57-33-北立面 (2)-20161220.JPG?op=CREATE&data=TRUE&guardian_access_token=ghRCDMu8XKV5svXj6qWz-CH11409.TDH");
        System.out.println(put_url);
        System.out.println(operation.upload(put_url, file_path));
//        operation.listDirs(list_url);
//        operation.download();
    }
    public String encoder(String url) throws UnsupportedEncodingException {
        StringBuilder resultURL = new StringBuilder();
        for (int i = 0; i < url.length(); i++) {
            char charAt = url.charAt(i);
            //只对汉字处理
            if (isChinese(charAt) || charAt == ' ') {
                String encode = URLEncoder.encode(charAt+"","UTF-8");
                resultURL.append(encode);
            }else {
                resultURL.append(charAt);
            }
        }
        return resultURL.toString();
    }
    public boolean isChineseChar(char c) {
        Character.UnicodeScript sc = Character.UnicodeScript.of(c);
        if (sc == Character.UnicodeScript.HAN) {
            return true;
        }
        return false;
//        return String.valueOf(c).matches("[\u4e00-\u9fa5]");
    }

    private static boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
            return true;
        }
        return false;
    }


}
