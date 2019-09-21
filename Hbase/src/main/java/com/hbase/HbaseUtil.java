package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HbaseUtil {
    private static HBaseAdmin hBaseAdmin;
    private static HConnection hConnection;

    static {
        String zoo ="master:2181,node1:2181,node2:2181";
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum",zoo);
        try {
            hBaseAdmin = new HBaseAdmin(conf);
            hConnection= HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String tablename, String familyname){
        //创建表的描述对象
        HTableDescriptor table = new HTableDescriptor(tablename);
        //创建列族的描述对象
        HColumnDescriptor family = new HColumnDescriptor(familyname);

        table.addFamily(family);

        try {
            hBaseAdmin.createTable(table);
            System.out.println(tablename+"创建成功");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void deleteTable(String tablename){
        try {
            //判断表是否存在，存在返回值为true，否则为false
            boolean table = hBaseAdmin.tableExists(tablename);
            //表存在则删除
            if (table){
                //hbase删除表必须先禁用
                hBaseAdmin.disableTable(tablename);
                //删除表
                hBaseAdmin.deleteTable(tablename);
                System.out.println(tablename+"已删除");
            }else {
                System.out.println("没有"+tablename);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void scanAll(String tablename, String familyname){
        try {
            HTableInterface student = hConnection.getTable(tablename);
            //创建scan对象
            Scan scan = new Scan();
            scan.addFamily(familyname.getBytes());
            //用迭代器接受查出的结果,ResultScanner是一个迭代器
            ResultScanner rs = student.getScanner(scan);
            Result res;

            while ((res=rs.next()) != null){
                List<Cell> cells = res.listCells();
                String id = "",name="",age="",gender="",clazz="";
                for (Cell cell : cells) {
                    id = Bytes.toString(CellUtil.cloneRow(cell));
                    //获取列名
                    String colname = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if (colname.equals("name")){
                        name = Bytes.toString(CellUtil.cloneValue(cell));
                    }else if (colname.equals("age")){
                        age = Bytes.toString(CellUtil.cloneValue(cell));
                    }else if (colname.equals("gender")){
                        gender = Bytes.toString(CellUtil.cloneValue(cell));
                    }else {
                        clazz = Bytes.toString(CellUtil.cloneValue(cell));
                    }
                }
                String line = id+","+name+","+age+","+gender+","+clazz;
                System.out.println(line);

            }


        } catch (IOException e) {
            e.printStackTrace();
        }



    }

    public static void closeAll(){
        try {
            hBaseAdmin.close();
            hConnection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
