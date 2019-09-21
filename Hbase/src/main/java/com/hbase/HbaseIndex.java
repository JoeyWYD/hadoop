package com.shujia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HbaseIndex {
    public static class IndexMapper extends TableMapper<Text, NullWritable>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //获取rowkey，即id
            String id = Bytes.toString(key.get());
            //获取班级
            String clazz = Bytes.toString(value.getValue("info".getBytes(),"clazz".getBytes()));
            //拼接新的rowkey
            String outkey = clazz+"_"+id;
            context.write(new Text(outkey), NullWritable.get());
        }
    }

    public static class IndexReucer extends TableReducer<Text, NullWritable, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String rowkey = key.toString();

            //创建Put对象
            Put put = new Put(rowkey.getBytes());
            put.add("info".getBytes(),"null".getBytes(),"".getBytes());

            context.write(NullWritable.get(),put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建连接配置
        String zoo ="master:2181,node1:2181,node2:2181";
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum",zoo);
        //创建Job
        Job job = Job.getInstance(conf, HbaseIndex.class.getSimpleName());
        job.setJarByClass(HbaseIndex.class);

        //创建scan对象
        Scan scan = new Scan();
        scan.addFamily("info".getBytes());

        //初始化map
        TableMapReduceUtil.initTableMapperJob("student1", scan, IndexMapper.class, Text.class, NullWritable.class, job);
        //初始化reduce
        TableMapReduceUtil.initTableReducerJob("student_index", IndexReucer.class, job);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        job.waitForCompletion(true);
    }
}
