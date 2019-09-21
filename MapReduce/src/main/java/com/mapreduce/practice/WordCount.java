package com.mapreduce.practice;

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

public class WordCount {




    public static class WCMap extends Mapper<LongWritable, Text,Text, IntWritable>{
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String words[] = line.split(",");
            for (String word : words) context.write(new Text(word),new IntWritable(1));
        }
    }

    public static class WCReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable value : values){
                sum += Integer.parseInt(value.toString());
            }
            context.write(key,new IntWritable(sum));
        }


    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,WordCount.class.getSimpleName());
        job.setJarByClass(WordCount.class);

        FileInputFormat.addInputPath(job, new Path("C:\\Users\\WuYid\\work\\workspace\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\input\\words.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\WuYid\\work\\workspace\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\output\\words"));

        job.setMapperClass(WordCount.WCMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCount.WCReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
