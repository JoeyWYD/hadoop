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

public class Distinct {

    /**
     *  hdfs dfs -rm -r /usr/input/man.txt
     *  hdfs dfs -put Run.txt /usr/input/
     *  hdfs dfs -rm -r /usr/output/Distinct
     *  hadoop jar MapReduce-1.0-SNAPSHOT.jar com.mapreduce.practice.Distinct /usr/input/man.txt /usr/output/Distinct
     *  hdfs dfs -cat /usr/output/Distinct/part-r-00000
     */



    public static class WCMap extends Mapper<LongWritable, Text,IntWritable,Text >{
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.contains("男"))
                context.write(new IntWritable(1),new Text(line));
            else
                context.write(new IntWritable(2),new Text(line));
        }
    }

    public static class WCReduce extends Reducer<IntWritable,Text,Text,Text>{
        protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            int sum=0;
            for (Text value : values){
                context.write(new Text((value.toString().contains("男")?"男":"女")+Integer.toString(++sum)+":"),value);
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {





        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Distinct.class.getSimpleName());
        job.setJarByClass(Distinct.class);

        FileInputFormat.addInputPath(job, new Path("C:\\Users\\WS\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\input\\man.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\WS\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\output\\man"));

        job.setMapperClass(Distinct.WCMap.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Distinct.WCReduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
