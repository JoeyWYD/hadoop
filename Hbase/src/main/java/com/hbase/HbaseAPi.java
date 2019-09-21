package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hbase.ReadUtil.*;

public class HbaseAPi {

    HBaseAdmin hBaseAdmin;
    HConnection hConnection;


    @Before
    public void init(){
        String zoo="192.168.52.135:2181,192.168.52.133:2181,192.168.52.134:2181";
        Configuration conf =new Configuration();
        conf.set("Hbase.zookeeper.quorum",zoo);



        try {
            hBaseAdmin = new HBaseAdmin(conf);
            hConnection = HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void create_table(){
        HTableDescriptor table = new HTableDescriptor("student");
        HColumnDescriptor family = new HColumnDescriptor("info");

        table.addFamily(family);
        family.setMaxVersions(5);


        try {
            hBaseAdmin.createTable(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void delete_table() throws IOException {
        try{
            boolean istable = hBaseAdmin.tableExists("student");
            if (istable) {
                hBaseAdmin.disableTable("user");
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
            HTableInterface table = hConnection.getTable("user");
            Put put = new Put("1500100002".getBytes());
            put.add("info".getBytes(),"name".getBytes(),"吕金鹏".getBytes());
            put.add("info".getBytes(),"age".getBytes(),"24".getBytes());
            put.add("info".getBytes(),"gender".getBytes(),"男".getBytes());
            put.add("info".getBytes(),"clazz".getBytes(),"文科六班".getBytes());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void get(){
        try {
            HTableInterface table = hConnection.getTable("user");
            Get get = new Get("1500100002".getBytes());
            get.addFamily("info".getBytes());
            Result  res = table.get(get);
            String name = Bytes.toString(res.getValue("info".getBytes(),"name".getBytes()));
            String age = Bytes.toString(res.getValue("info".getBytes(),"age".getBytes()));
            String gender = Bytes.toString(res.getValue("info".getBytes(),"gender".getBytes()));
            String clazz = Bytes.toString(res.getValue("info".getBytes(),"clazz".getBytes()));
            System.out.printf(name+"\t"+age+"\t"+gender+"\t"+clazz+"\t");

            List<Cell> cells = res.listCells();
            for (Cell cell :cells){
                String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
                String colName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(familyName+":"+colName+":"+"\t"+value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void insert_all(){
        try {
            HTableInterface table = hConnection.getTable("student");
            String path = "C:\\Users\\WS\\IdeaProjects\\Hbase\\src\\data\\students.txt";
            ArrayList<Student> students = ReadUtil.read(path, Student.class);
            Put put;
            for (Student student : students) {
                String id = student.getId();
                put = new Put(id.getBytes());
                put.add("info".getBytes(),"name".getBytes(),student.getName().getBytes());
                put.add("info".getBytes(),"age".getBytes(),Bytes.toBytes(student.getAge()));
                put.add("info".getBytes(),"gender".getBytes(),student.getGender().getBytes());
                put.add("info".getBytes(),"clazz".getBytes(),student.getClazz().getBytes());
                table.put(put);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void scan_all(){
        try {
            HTableInterface student = hConnection.getTable("user");
            Scan scan = new Scan();
            scan.addFamily("info".getBytes());
            ResultScanner rs = student.getScanner(scan);
            Result res;
            while ((res=rs.next()) != null){
                String rowkey = Bytes.toString(res.getRow());
                String name = Bytes.toString(res.getValue("info".getBytes(),"name".getBytes()));
                Integer age = Bytes.toInt(res.getValue("info".getBytes(),"age".getBytes()));
                String gender = Bytes.toString(res.getValue("info".getBytes(),"gender".getBytes()));
                String clazz = Bytes.toString(res.getValue("info".getBytes(),"clazz".getBytes()));
                System.out.println(rowkey+"\t"+name+"\t"+age+"\t"+gender+"\t"+clazz);

                List<Cell> cells = res.listCells();
                for (Cell cell :cells){
                    String key = Bytes.toString(CellUtil.cloneRow(cell));
                    String colName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    System.out.print(key+":");
                    if (colName.equals("age")) {
                        Integer age2 = Bytes.toInt(CellUtil.cloneValue(cell));
                        System.out.println(age2);
                    } else {
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        System.out.println(value);
                    }
                }





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
