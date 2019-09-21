package com.hdfs.emp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;

public class appendahdsj2 {

    /*
    *
    * 编写一个Java程序，向HDFS中ahdsj1.csv文件追加数据ahdsj2.csv
    *
     */


    public static boolean test(Configuration conf,String path) throws IOException {
        try {
            FileSystem fs = FileSystem.get(conf);
            return fs.exists(new Path((path)));
        } catch (IOException e){
            e.printStackTrace();
            return false;
        }
    }

    public static void copyFromLocalFile(Configuration conf,String localFilePath,String remoteFilePath) throws IOException {
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        try{
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(false,true,localPath,remotePath);
        } catch (IOException e ){
            e.printStackTrace();
        }
    }

    public static void appendToFile(Configuration conf,String localFilePath,String remoteFilePath) throws IOException{
        Path remotePath = new Path(remoteFilePath);
        try {
            FileSystem fs = FileSystem.get(conf);
            FileInputStream in = new FileInputStream(localFilePath);
            FSDataOutputStream out = fs.append(remotePath);
            byte[] data = new byte[1024];
            int read = -1;
            while ((read = in.read(data))>0) out.write(data,0,read);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void  main(String args[]) throws IOException {
        Configuration conf = new Configuration();
        String localFilePath = "/root/output/ahdsjjs2.csv";
        String remoteFilePath = "/usr/hadoop/ahdsjjs1.csv";
        String choice = "";
        try{
            Boolean fillExists = false;
            if (appendahdsj2.test(conf,remoteFilePath)) {
                fillExists = true;
                System.out.println(remoteFilePath+"已存在");
                choice = "append";
            }
            else {
                System.out.println(remoteFilePath+"不存在");
                choice = "overwrite";
            }
            if (!fillExists) {
                appendahdsj2.copyFromLocalFile(conf,localFilePath,remoteFilePath);
                System.out.println(localFilePath+"已上传到"+remoteFilePath);
            } else if (choice.equals("append")) {
                appendahdsj2.appendToFile(conf, localFilePath, remoteFilePath);
                System.out.println(localFilePath + "已追加至" + remoteFilePath);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


}
