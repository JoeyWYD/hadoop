package com.shujia.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseApi {

    HBaseAdmin hBaseAdmin;
    HConnection connection;

    @Before
    public void init() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master:2181,node1:2181,node2:2181");


        try {
            //连接Master 创建hbase操作对象，主要用来创建，修改，删除表
            hBaseAdmin = new HBaseAdmin(conf);

//           //丽娜姐regionserver,  负责表的增删改查
            connection = HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createTable() {
        //创建表得描述对象
        HTableDescriptor student = new HTableDescriptor("user1");

        //创建列蹴得描述对象
        HColumnDescriptor info = new HColumnDescriptor("info");

        info.setMaxVersions(5);//版本数
        info.setTimeToLive(5);//过期时间
        info.setInMemory(true);

        student.addFamily(info);
        //创建表
        try {
            hBaseAdmin.createTable(student);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dropTable() {
        try {

            if (hBaseAdmin.tableExists("student")) {
                System.out.println("表存在");
            }

            //1、让表失效
            hBaseAdmin.disableTable("student");
            //hBaseAdmin.enableTable("student");//让表生效
            //2、删除表
            hBaseAdmin.deleteTable("student");


            if (!hBaseAdmin.tableExists("student")) {
                System.out.println("表不存在");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入一行数据
     */
    @Test
    public void insert() {
        try {
            //获取表的实例对象
            HTableInterface student = connection.getTable("student");

            //创建put对象，指定rowkey
            Put put = new Put("15001001003".getBytes());

            //插入一列
            put.add("info".getBytes(), "name".getBytes(), "张三".getBytes());

            //Bytes.toBytes   将基本数据类型转成字节数组
            put.add("info".getBytes(), "age".getBytes(), Bytes.toBytes(12));

            put.add("info".getBytes(), "gender".getBytes(), "男".getBytes());
            put.add("info".getBytes(), "clazz".getBytes(), "文科一班".getBytes());

            student.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void query() {
        //获取表的实例对象
        try {
            HTableInterface student = connection.getTable("student");
            //创建gei对象
            Get get = new Get("001".getBytes());
            get.addFamily("info".getBytes());

            //查询数据
            Result result = student.get(get);

            //通过列蔟加上列获取数据，返回一个字节数组
            String name = Bytes.toString(result.getValue("info".getBytes(), "name".getBytes()));
            int age = Bytes.toInt(result.getValue("info".getBytes(), "age".getBytes()));
            String gender = Bytes.toString(result.getValue("info".getBytes(), "gender".getBytes()));

            System.out.println(name + "\t" + age + "\t" + gender);


            //获取所有单元格
            List<Cell> cells = result.listCells();

            for (Cell cell : cells) {

                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                System.out.println(family);
                //获取单元格列的名称
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));

                if ("age".equals(qualifier)) {
                    int a = Bytes.toInt(CellUtil.cloneValue(cell));
                    System.out.println(a);
                } else {
                    String s = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(s);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //删除数据
    @Test
    public void deleteData() {
        try {
            HTableInterface student = connection.getTable("student");

            Delete delete = new Delete("001".getBytes());
            //delete.deleteFamily("info".getBytes());
            student.delete(delete);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量插入数据
     */

    @Test
    public void insertAll() {
        try {
            HTableInterface student = connection.getTable("student");

            List<Put> puts = new ArrayList<Put>();

            //读取所有学生数据
            ArrayList<Student> students = ReadUtil.read("E:\\第二期\\code\\hbase\\data\\students.txt", Student.class);

            for (Student student1 : students) {
                Put put = new Put(student1.getId().getBytes());
                put.add("info".getBytes(), "name".getBytes(), student1.getName().getBytes());
                put.add("info".getBytes(), "age".getBytes(), Bytes.toBytes(student1.getAge()));
                put.add("info".getBytes(), "gender".getBytes(), student1.getGender().getBytes());
                put.add("info".getBytes(), "clazz".getBytes(), student1.getClazz().getBytes());
                puts.add(put);
            }


            //哈如多行数据
            student.put(puts);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * scan
     */
    @Test
    public void scan() {
        try {
            HTableInterface student = connection.getTable("student");

            //构建scan对象
            Scan scan = new Scan();
            //增加需要扫描的列簇，列也可以增加
            scan.addFamily("info".getBytes());

            //增加扫描的开始key和结束key
            scan.setStartRow("1500100916".getBytes());
            scan.setStopRow("1500100918".getBytes());

            //执行扫描，返回一个迭代器
            ResultScanner scanner = student.getScanner(scan);

            Result result;


            //一行一行迭代
            while ((result = scanner.next()) != null) {

                //获取rowkey
                String id = Bytes.toString(result.getRow());
                String name = Bytes.toString(result.getValue("info".getBytes(), "name".getBytes()));

                int age = Bytes.toInt(result.getValue("info".getBytes(), "age".getBytes()));

                String gendet = Bytes.toString(result.getValue("info".getBytes(), "gender".getBytes()));
                String clazz = Bytes.toString(result.getValue("info".getBytes(), "clazz".getBytes()));
                System.out.println(id + "\t" + name + "\t" + age + "\t" + gendet + "\t" + clazz);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 扫描文科一班的学生
     */
    @Test
    public void filter() {
        try {
            HTableInterface student = connection.getTable("student");

            //构建scan对象
            Scan scan = new Scan();
            //增加需要扫描的列簇，列也可以增加
            scan.addFamily("info".getBytes());

            //增加过滤器，过滤文科一班的学生
            RegexStringComparator regexStringComparator = new RegexStringComparator("文科一班");
            SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter("info".getBytes(), "clazz".getBytes(), CompareFilter.CompareOp.EQUAL, regexStringComparator);


            /**
             * 列值过滤器：全表扫描
             *
             */
            SubstringComparator comp = new SubstringComparator("男");
            SingleColumnValueFilter columnValueFilter1 = new SingleColumnValueFilter("info".getBytes(), "gender".getBytes(), CompareFilter.CompareOp.EQUAL, comp);


            //前缀过滤
            BinaryPrefixComparator prefixComparator = new BinaryPrefixComparator(Bytes.toBytes("终"));

            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, prefixComparator);


            /**
             * rowkey前缀过滤
             *
             */
            /*BinaryPrefixComparator binaryPrefixComparator = new BinaryPrefixComparator("15001002".getBytes());
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, binaryPrefixComparator);*/

            //过滤器集合
            FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            fl.addFilter(columnValueFilter);
            fl.addFilter(columnValueFilter1);
            fl.addFilter(filter);

            //增加过滤器
            scan.setFilter(fl);


            //执行扫描，返回一个迭代器
            ResultScanner scanner = student.getScanner(scan);

            Result result;


            //一行一行迭代
            while ((result = scanner.next()) != null) {

                //获取rowkey
                String id = Bytes.toString(result.getRow());
                String name = Bytes.toString(result.getValue("info".getBytes(), "name".getBytes()));

                int age = Bytes.toInt(result.getValue("info".getBytes(), "age".getBytes()));

                String gendet = Bytes.toString(result.getValue("info".getBytes(), "gender".getBytes()));
                String clazz = Bytes.toString(result.getValue("info".getBytes(), "clazz".getBytes()));
                System.out.println(id + "\t" + name + "\t" + age + "\t" + gendet + "\t" + clazz);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 建立列的索引
     */
    @Test
    public void insertindex() {
        try {
            HTableInterface student = connection.getTable("student");
            List<Put> puts = new ArrayList<Put>();

            //读取所有学生数据
            ArrayList<Student> students = ReadUtil.read("E:\\第二期\\code\\hbase\\data\\students.txt", Student.class);

            for (Student student1 : students) {

                //将班级当道rowkey里面，后面在对班级做过滤的时候速度会块很多，导致数据冗余
                String rowkey = student1.getClazz().substring(0, 2) + "_" + student1.getId();

                Put put = new Put(rowkey.getBytes());

                put.add("info".getBytes(), "name".getBytes(), student1.getName().getBytes());
                put.add("info".getBytes(), "age".getBytes(), Bytes.toBytes(student1.getAge()));
                put.add("info".getBytes(), "gender".getBytes(), student1.getGender().getBytes());
                put.add("info".getBytes(), "clazz".getBytes(), student1.getClazz().getBytes());
                puts.add(put);
            }

            //哈如多行数据
            student.put(puts);


        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    /**
     * row 前缀过滤
     */
    @Test
    public void queryClazz() {
        try {
            HTableInterface student = connection.getTable("student");

            Scan scan = new Scan();
            scan.addFamily("info".getBytes());
            /**
             * rowkey前缀过滤
             *
             */
            BinaryPrefixComparator binaryPrefixComparator = new BinaryPrefixComparator("文科".getBytes());
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, binaryPrefixComparator);

            scan.setFilter(rowFilter);


            Result result;
            ResultScanner scanner = student.getScanner(scan);

            //一行一行迭代
            while ((result = scanner.next()) != null) {

                //获取rowkey
                String id = Bytes.toString(result.getRow()).split("_")[1];
                String name = Bytes.toString(result.getValue("info".getBytes(), "name".getBytes()));

                int age = Bytes.toInt(result.getValue("info".getBytes(), "age".getBytes()));

                String gendet = Bytes.toString(result.getValue("info".getBytes(), "gender".getBytes()));
                String clazz = Bytes.toString(result.getValue("info".getBytes(), "clazz".getBytes()));
                System.out.println(id + "\t" + name + "\t" + age + "\t" + gendet + "\t" + clazz);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @After
    public void close() {
        //关闭连接
        try {
            hBaseAdmin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
