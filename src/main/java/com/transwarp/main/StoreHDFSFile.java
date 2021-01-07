package com.transwarp.main;

import com.transwarp.hdfs.HDFSOperation;
import org.apache.hadoop.fs.FileStatus;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
public class StoreHDFSFile {
    private Properties properties = new Properties();
    public static void main(String[] args) {
        if(args.length < 1){
            System.out.println("Need config file!");
            System.exit(1);
        }
        long startTime=System.currentTimeMillis();
        StoreHDFSFile op = new StoreHDFSFile();
        op.process(args[0]);
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");

    }
    private void process(String config){
        getProperties(config);
        HDFSOperation hdfsOperation = new HDFSOperation(properties);
        HDFSToHbaseOperation hdfsToHbaseOperation = new HDFSToHbaseOperation(properties);
        try{
            FileStatus[] dirs = hdfsOperation.listDir(properties.getProperty("base_path", "/voice"));
            System.out.println(properties.getProperty("base_path"));
            System.out.println(dirs.length);
            for(FileStatus dir : dirs){
                List<FileStatus> files = hdfsOperation.getAllFileList(dir.getPath());
                int file_size = files.size();
                int cur = 0;
                for(FileStatus file: files){
                    System.out.println("####################################");
                    System.out.println("============" + (cur + 1) + "/" + file_size + "================");
                    System.out.println("正在获取目录："+ dir.getPath().getParent() + "/" + dir.getPath().getName() + "下的文件");
                    byte[] data = hdfsOperation.download(file.getPath());
                    hdfsToHbaseOperation.loadHDFSFileToHbase(file, data);
                    System.out.println("文件名为：" +file.getPath().getName()+"，文件大小为：" + data.length / (2014) + "K");
                    System.out.println("####################################");
                    System.out.println();
                    cur ++;
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

    }
    public void getProperties(String path){
        try{
            InputStream in =  new BufferedInputStream(new FileInputStream(path));
            properties.load(in);
        }catch (IOException e){
            e.printStackTrace();
        }
    }


}
