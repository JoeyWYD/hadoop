package com.shujia;

import Util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HbaseIndexQuery {
    public static void main(String[] args) throws IOException {

        String zoo ="master:2181,node1:2181,node2:2181";
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum",zoo);

        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        HConnection hconnection = HConnectionManager.createConnection(conf);

        //查询索引表
        HTableInterface student_index = hconnection.getTable("student_index2");
        Scan scan = new Scan();
        scan.addFamily("info".getBytes());

        //建立文科前缀过滤器
        BinaryPrefixComparator bpc = new BinaryPrefixComparator("文科".getBytes());
        //建立rowkey过滤器
        RowFilter rf1 = new RowFilter(CompareFilter.CompareOp.EQUAL, bpc);

        SubstringComparator ssc2 = new SubstringComparator("女");
        RowFilter rf2 = new RowFilter(CompareFilter.CompareOp.EQUAL, ssc2);

        FilterList filterList = new FilterList();
        filterList.addFilter(rf1);
        filterList.addFilter(rf2);
        scan.setFilter(filterList);

        //执行查询
        ResultScanner rs = student_index.getScanner(scan);
        Result res;

        //创建集合用来接收查询后的rowkey(id)
        ArrayList<String> rowkeys = new ArrayList<>();


        while ((res=rs.next()) != null){
            //获取二级索引表中的rowkey
            String key = Bytes.toString(res.getRow());
            //切分出原表中的rowkey
            String rowkey = key.split("_")[2];
            rowkeys.add(rowkey);
        }

        //遍历原表rowkey集合，获取rowkey对应的列值
        for (String rowkey : rowkeys) {
            HbaseUtil.get("student1",rowkey);
        }
        HbaseUtil.closeAll();
    }
}
