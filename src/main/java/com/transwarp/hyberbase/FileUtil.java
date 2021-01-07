package com.transwarp.hyberbase;

import javax.imageio.stream.FileImageInputStream;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;

public class FileUtil {
    public static ArrayList<File> getListFiles(Object obj) {
        File directory = null;
        if (obj instanceof File) {
            directory = (File) obj;
        } else {
            directory = new File(obj.toString());
        }
        ArrayList<File> files = new ArrayList<File>();
        if (directory.isFile()) {
            files.add(directory);
            return files;
        } else if (directory.isDirectory()) {
            File[] fileArr = directory.listFiles();
            for (int i = 0; i < fileArr.length; i++) {
                File fileOne = fileArr[i];
                files.addAll(getListFiles(fileOne));
            }
        }else {
            System.err.println("该文件不存在！");
        }
        return files;
    }
    public static boolean isPictrueExist(String dirPath) {
        File file = new File(dirPath);
        if (file.exists()) {
            return true;
        }
        return false;
    }

    public static byte[] localImage2bytes_base64(String path){
        byte[] data = localImage2byte(path);
        final Base64.Encoder encoder = Base64.getEncoder();
        return encoder.encode(data);
    }

    //图片到byte数组
    public static byte[] localImage2byte(String path){
        byte[] data = null;
        FileImageInputStream input = null;
        try {
            input = new FileImageInputStream(new File(path));
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int numBytesRead = 0;
            while ((numBytesRead = input.read(buf)) != -1) {
                output.write(buf, 0, numBytesRead);
            }
            data = output.toByteArray();
            output.close();
            input.close();
        }
        catch (FileNotFoundException ex1) {
            ex1.printStackTrace();
        }
        catch (IOException ex1) {
            ex1.printStackTrace();
        }
        return data;
    }

    public static byte [] onlineImg2Bytes(String u) throws IOException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        URL url = new URL(u);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5*1000);
        InputStream in = conn.getInputStream();
        byte[] buffer = new byte[1024];
        int len = 0;
        while ((len = in.read(buffer)) != -1) {
            out.write(buffer, 0, len);
        }
        out.flush();
        in.close();
        conn.disconnect();

        return out.toByteArray();

    }
    // file转byte
    public static byte[] file2Byte(File file) throws Exception {
        FileInputStream fileInputStream = new FileInputStream(file);
        byte[] data = new byte[fileInputStream.available()];
        fileInputStream.read(data);
        fileInputStream.close();
        return data;
    }
    // 16进制转2进制
    public static byte[] hexStr2Byte(String hex) {
		ByteBuffer bf = ByteBuffer.allocate(hex.length() / 2);
		for (int i = 0; i < hex.length(); i++) {
			String hexStr = hex.charAt(i) + "";
			i++;
			hexStr += hex.charAt(i);
			byte b = (byte) Integer.parseInt(hexStr, 16);
			bf.put(b);
		}
		return bf.array();
	}
    // byte转file
    public static File byte2File(byte[] bytes, String path, String fileName) {
        if (bytes == null)
            return null;

        File file = new File(path + "\\" + fileName);
        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(file);
            fileOutputStream.write(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileOutputStream != null) {
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return file;
    }
}