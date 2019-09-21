package com.mapreduce.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class ReduceJoin {
    public static class Map_Rjoin extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            //获取切片信息中的路径信息
            String path = inputSplit.getPath().toString();
            if (path.contains("pm25")){
                String[] splits = value.toString().split(",");
                if(splits.length == 3){
                    String mkey = splits[1];
                    String mvalue = "pm"+splits[0]+"\t"+splits[2];
                    context.write(new Text(mkey), new Text(mvalue));
                }
            }else if(path.contains("city")){
                String[] splits = value.toString().split(" ");
                if (splits.length == 5){
                    String mkey = splits[0];
                    String mvalue = "city"+splits[1]+"\t"+splits[2]+"\t"+splits[3]+"\t"+splits[4];
                    context.write(new Text(mkey), new Text(mvalue));
                }
            }

        }
    }

    public static class Reduce_Rjoin extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> pm = new ArrayList<String>();
            ArrayList<String> city = new ArrayList<String>();
            for (Text value : values) {
                String line = value.toString();
                if (line.startsWith("pm")){
                    String outv = line.substring(2);
                    pm.add(outv);
                }else if(line.startsWith("city")){
                    String outv = line.substring(4);
                    city.add(outv);
                }
            }

            for (String s1 : pm) {
                for (String s2 : city) {
                    context.write(key, new Text(s1+"\t"+s2));
                }
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置文件
        Configuration conf = new Configuration();
        //创建任务
        Job job = Job.getInstance(conf, ReduceJoin.class.getSimpleName());
        //指定jar包
        job.setJarByClass(ReduceJoin.class);
        //指定输入输出路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //指定map类
        job.setMapperClass(Map_Rjoin.class);
        //指定map输出键值
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //指定reduce类
        job.setReducerClass(Reduce_Rjoin.class);
        //指定reduce输出键值
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //提交任务
        job.waitForCompletion(true);
    }
}
