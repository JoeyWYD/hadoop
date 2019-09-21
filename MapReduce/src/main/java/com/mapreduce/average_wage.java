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

public class average_wage {

    public static class wageMap extends Mapper<LongWritable, Text,Text, IntWritable>{
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            context.write(new Text(words[0]), new IntWritable(Integer.valueOf(words[2])));
        }
    }


    public static class wageReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            int j = 0;
            for (IntWritable value : values) {
                sum += value.get();
                j++;
            }
            context.write(key, new IntWritable(sum / j));
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建配置文件
        Configuration conf = new Configuration();
        //创建任务
        Job job = Job.getInstance(conf, average_wage.class.getSimpleName());
        //指定jar包
        job.setJarByClass(average_wage.class);
        //指定输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //指定map类
        job.setMapperClass(wageMap.class);
        //指定map输出键值的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定reduce类
        job.setReducerClass(wageReduce.class);
        //指定reduce输出键值的数据类型的类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(wageReduce.class);
        //提交任务
        job.waitForCompletion(true);

    }

}
