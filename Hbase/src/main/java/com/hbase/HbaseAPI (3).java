package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseAPI {
    HBaseAdmin hBaseAdmin;
    HConnection hConnection;


    @Before
    public void init(){
        String zoo ="master:2181,node1:2181,node2:2181";
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum",zoo);

        try {
            //连接Master 创建hbase操作对象，主要用来创建，修改，删除表
            hBaseAdmin = new HBaseAdmin(conf);
            //连接regionserver,  负责表的增删改查
            hConnection = HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void create_table(){
        //创建表的描述对象
        HTableDescriptor user = new HTableDescriptor("user");
        //创建列族的描述对象
        HColumnDescriptor info = new HColumnDescriptor("info");
        //通过列族的链接我们可以为列族设置各种参数
        info.setMaxVersions(5);
        //增加列族
        user.addFamily(info);

        try {
            hBaseAdmin.createTable(user);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void delete_table(){
        try {
            //判断表是否存在，存在返回值为true，否则为false
            boolean user = hBaseAdmin.tableExists("user");
            //表存在则删除
            if (user){
                //hbase删除表必须先禁用
                hBaseAdmin.disableTable("user");
                //删除表
                hBaseAdmin.deleteTable("user");
                System.out.println("表已删除");
            }else {
                System.out.println("没有此表");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void insert(){
        try {
            //创建表的对象
            HTableInterface user = hConnection.getTable("user");
            //创建put对象，同时制定要put的rowkey
            Put put = new Put("1500100002".getBytes());
            //上传value
            put.add("info".getBytes(),"name".getBytes(),"吕金鹏".getBytes());
            put.add("info".getBytes(),"age".getBytes(),"24".getBytes());
            put.add("info".getBytes(),"gender".getBytes(),"男".getBytes());
            put.add("info".getBytes(),"clazz".getBytes(),"文科六班".getBytes());

            user.put(put);


        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    @Test
    public void get(){

        try {
            //创建表的对象
            HTableInterface user = hConnection.getTable("user");
            Get get = new Get("1500100002".getBytes());
            get.addFamily("info".getBytes());
            //用Result获取列族中的数据
            Result res = user.get(get);
            String name = Bytes.toString(res.getValue("info".getBytes(),"name".getBytes()));
            String age = Bytes.toString(res.getValue("info".getBytes(),"age".getBytes()));
            String gender = Bytes.toString(res.getValue("info".getBytes(),"gender".getBytes()));
            String clazz = Bytes.toString(res.getValue("info".getBytes(),"clazz".getBytes()));

            System.out.println(name +"\t"+ age +"\t"+ gender +"\t"+ clazz);

            List<Cell> cells = res.listCells();
            for (Cell cell : cells) {
                //获取列族名
                String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
                //获取列名
                String colName = Bytes.toString(CellUtil.cloneQualifier(cell));
                //获取值
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(familyName+":"+colName+"\t"+value);

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void insert_all(){
        try {
            //获取表的对象
            HTableInterface student = hConnection.getTable("student");
            //用自定义对象Student读取文件并放入集合
            String path = "F:\\workspace_eclipse\\motonghua\\Hbase\\src\\data\\students.txt";
            ArrayList<Student> students =ReadUtil.read(path, Student.class);
            //创建put对象
            Put put;
            for (Student stu : students) {
                //获取rowkey
                String id = stu.getId();
                put = new Put(id.getBytes());
                //写入数据
                put.add("info".getBytes(),"name".getBytes(),stu.getName().getBytes());
                put.add("info".getBytes(),"age".getBytes(),Bytes.toBytes(stu.getAge()));
                put.add("info".getBytes(),"gender".getBytes(),stu.getGender().getBytes());
                put.add("info".getBytes(),"clazz".getBytes(),stu.getClazz().getBytes());

                student.put(put);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void scan_all(){
        try {
            HTableInterface student = hConnection.getTable("student");
            //创建scan对象
            Scan scan = new Scan();
            scan.addFamily("info".getBytes());
            //用迭代器接受查出的结果,ResultScanner是一个迭代器
            ResultScanner rs = student.getScanner(scan);
            Result res;

            while ((res=rs.next()) != null){
                //获取rowkey
                String rowkey = Bytes.toString(res.getRow());
                //获取列族中的值
                String name = Bytes.toString(res.getValue("info".getBytes(),"name".getBytes()));
                Integer age = Bytes.toInt(res.getValue("info".getBytes(),"age".getBytes()));
                String gender = Bytes.toString(res.getValue("info".getBytes(),"gender".getBytes()));
                String clazz = Bytes.toString(res.getValue("info".getBytes(),"clazz".getBytes()));

                System.out.println(rowkey+"\t"+name+"\t"+age+"\t"+gender+"\t"+clazz);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @After
    public void after(){
        try {
            hBaseAdmin.close();
            hConnection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
