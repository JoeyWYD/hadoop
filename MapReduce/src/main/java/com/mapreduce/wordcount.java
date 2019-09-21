package com.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class wordcount {
    public static class WCMap extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            line = line.replace("!","");
            line = line.replace(",","");
            line = line.replace(".","");
            line = line.replace("\"","");
            line = line.replace("!","");
            line = line.replace(":","");
            String [] words = line.split(" ");
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
                context.write(new Text("SumWords:"),new IntWritable(1));
            }
        }
    }

    public static class WCReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));

        }
    }

    public static class WCReduce2 extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum+1000));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建配置文件
        Configuration conf = new Configuration();
        //创建任务
        Job job = Job.getInstance(conf, wordcount.class.getSimpleName());
        //指定jar包
        job.setJarByClass(wordcount.class);
        //指定输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //指定map类
        job.setMapperClass(WCMap.class);
        //指定map输出键值的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定reduce类
        job.setReducerClass(WCReduce.class);
        job.setReducerClass(WCReduce2.class);
        //指定reduce输出键值的数据类型的类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //提交任务
        job.waitForCompletion(true);

    }

}
