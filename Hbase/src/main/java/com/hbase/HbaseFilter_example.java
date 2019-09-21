package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HbaseFilter_example {
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
    public void filter_colvalue(){
        try {
            HTableInterface student = hConnection.getTable("student");
            //创建scan对象
            Scan scan = new Scan();
            scan.addFamily("info".getBytes());

            //字符串过滤
            SubstringComparator ssc = new SubstringComparator("文科");
            //指定以列值为过对象
            SingleColumnValueFilter scvf1 = new SingleColumnValueFilter("info".getBytes(), "clazz".getBytes(), CompareFilter.CompareOp.EQUAL, ssc);
//            //导入过滤器
//            scan.setFilter(scvf);


            //前缀过滤
            BinaryPrefixComparator bpc = new BinaryPrefixComparator("高".getBytes());
            SingleColumnValueFilter scvf2 = new SingleColumnValueFilter("info".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, bpc);

            //过滤器列表
            FilterList filterList = new FilterList();
            //添加进过滤器列表
            filterList.addFilter(scvf1);
            filterList.addFilter(scvf2);

            scan.setFilter(filterList);


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

    @Test
    public void filter_row(){
        try {
            HTableInterface student = hConnection.getTable("student");
            Scan scan = new Scan();
            scan.addFamily("info".getBytes());

            //前缀过滤器
            BinaryPrefixComparator bpc = new BinaryPrefixComparator("15001002".getBytes());
            //指定以rowkey过滤
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, bpc);

            //字符串过滤
            SubstringComparator ssc = new SubstringComparator("文科");
            //指定以列值为过对象
            SingleColumnValueFilter scvf1 = new SingleColumnValueFilter("info".getBytes(), "clazz".getBytes(), CompareFilter.CompareOp.EQUAL, ssc);


            FilterList filterList = new FilterList();
            filterList.addFilter(rowFilter);
            filterList.addFilter(scvf1);
            scan.setFilter(filterList);

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
