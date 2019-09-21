package com.shujia;

import Test.HdfsToHbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class HdfsToHbase2 {
    public static class ToHbaseMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String rowkey = value.toString().split(",")[0];
            context.write(new Text(rowkey), value);
        }
    }


    public static class ToHbaseReducer extends TableReducer<Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put put;
            for (Text value : values) {
                String[] stu = value.toString().split(",");
                if (stu.length == 5) {
                    //创建put对象,并传入rowkey
                    put = new Put(stu[0].getBytes());
                    //将各列的值写入put对象中
                    put.add("info".getBytes(), "name".getBytes(), stu[1].getBytes());
                    put.add("info".getBytes(), "age".getBytes(), stu[2].getBytes());
                    put.add("info".getBytes(), "gender".getBytes(), stu[3].getBytes());
                    put.add("info".getBytes(), "clazz".getBytes(), stu[4].getBytes());


                    context.write(NullWritable.get(),put);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建连接配置
        String zoo ="master:2181,node1:2181,node2:2181";
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum",zoo);
        //创建job
        Job job = Job.getInstance(conf, HdfsToHbase2.class.getSimpleName());
        //指定jar包
        job.setJarByClass(HdfsToHbase2.class);
        //指定mapper类，以及Mapper的输出键值类型
        job.setMapperClass(ToHbaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //指定reducer
        TableMapReduceUtil.initTableReducerJob("student1", ToHbaseReducer.class, job);
        //指定reduce输出键值
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        job.waitForCompletion(true);
    }

}
