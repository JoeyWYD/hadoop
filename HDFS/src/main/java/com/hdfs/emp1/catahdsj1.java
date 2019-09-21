package com.hdfs.emp1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class catahdsj1 {

    /**
     *
     *编写一个Java程序，使用字符流ahdsj1.csv文件读取数据
     *
     */

    public static void cat (Configuration conf, String remoteFilePath) throws IOException {
        Path remotePath = new Path(remoteFilePath);
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(remotePath);
        BufferedReader d = new BufferedReader( new InputStreamReader(in));
        String line = null;
        StringBuffer buffer = new StringBuffer();
        while ((line = d.readLine()) != null)
        { buffer.append(line); }
        String res = buffer.toString();
        FileWriter f1=new FileWriter("/root/output/cat.txt");
        f1.write(res);
        f1.close();
    }


    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String remoteFilePath = "/user/hadoop/ahdsjjs1.csv";
        try {
            System.out.println("读取文件: " + remoteFilePath);
            catahdsj1.cat(conf, remoteFilePath);
            System.out.println("\n读取完成");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
