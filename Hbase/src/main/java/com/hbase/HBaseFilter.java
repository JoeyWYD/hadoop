package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseFilter {


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
    public void filter_colvalue(){
        try {
            HTableInterface student = hConnection.getTable("user");
            Scan scan = new Scan();
            scan.addFamily("info".getBytes());

//            字符串过滤
            SubstringComparator ssc = new SubstringComparator("文科");
            SingleColumnValueFilter scvf1 = new SingleColumnValueFilter("info".getBytes(), "clazz".getBytes(), CompareFilter.CompareOp.EQUAL, ssc);
//            scan.setFilter(scvf1);

//            前缀过滤
            BinaryPrefixComparator bpc = new BinaryPrefixComparator("高".getBytes());
            SingleColumnValueFilter scvf2 = new SingleColumnValueFilter("info".getBytes(), "clazz".getBytes(), CompareFilter.CompareOp.EQUAL, bpc);
            FilterList filterList = new FilterList();
            filterList.addFilter(scvf1);
            filterList.addFilter(scvf2);

            scan.setFilter(filterList);

            ResultScanner rs = student.getScanner(scan);
            Result res;
            while ((res=rs.next()) != null){
                String rowkey = Bytes.toString(res.getRow());
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
    public void filler_row(){
        try {
            HTableInterface student = hConnection.getTable("user");
            Scan scan = new Scan();
            scan.addFamily("info".getBytes());
//            前缀过滤
            BinaryPrefixComparator bpc = new BinaryPrefixComparator("15001002".getBytes());
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, bpc);
//            scan.setFilter(rowFilter);


//            字符串过滤

            SubstringComparator ssc = new SubstringComparator("文科");
            SingleColumnValueFilter scvf1 = new SingleColumnValueFilter("info".getBytes(), "clazz".getBytes(), CompareFilter.CompareOp.EQUAL, ssc);

            FilterList filterList = new FilterList();
            filterList.addFilter(rowFilter);
            filterList.addFilter(scvf1);

            scan.setFilter(filterList);

            ResultScanner rs = student.getScanner(scan);
            Result res;
            while ((res=rs.next()) != null){
                String rowkey = Bytes.toString(res.getRow());
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
