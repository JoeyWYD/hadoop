package com.hdfs.emp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class downloadahdsj1 {

    /**
     *
     * 编写一个Java程序，从HDFS中下载ahdsj1.csv文件到本地
     *
     */


    public static void copyToLocal(Configuration conf, String remoteFilePath, String localFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        File f = new File(localFilePath);

        if (f.exists()) {
            System.out.println(localFilePath + " 已存在.");
            Integer i = 0;
            while (  true) {
                f = new File(  localFilePath + "_" + i.toString()     );
                if (!f.exists()  ) {
                    localFilePath = localFilePath + "_" + i.toString()      ;
                    break;
                }
            }
            System.out.println("将重新命名为: " + localFilePath);
        }


        Path localPath = new Path(localFilePath);
        fs.copyToLocalFile(remotePath, localPath);
        fs.close();

    }


    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String localFilePath = "/root/output/down.csv";
        String remoteFilePath = "/user/hadoop/ahdsjjs1.csv";

        try {
            downloadahdsj1.copyToLocal(conf, remoteFilePath, localFilePath);
            System.out.println("下载完成");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
