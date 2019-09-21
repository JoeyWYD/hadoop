package com.mapreduce.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;





public class MaxPm25InOneDay {

    public static class WCMap extends Mapper<LongWritable, Text,Text, Text>{
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.split(",").length==3) {
                String words[] = line.split(",");
                context.write(new Text(words[0]), new Text(words[1] + "_" + words[2]));
            }
        }
    }

    public static class WCReduce extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            int sum=0;
            String str="";
            for ( Text value : values){
                String words[]=value.toString().split("_");
                if (Integer.parseInt(words[1])>sum){
                    sum=Integer.parseInt(words[1]);
                    str=value.toString();
                }
            }
            context.write(key,new Text(str));
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, com.mapreduce.practice.MaxPm25InOneDay.class.getSimpleName());
        job.setJarByClass(com.mapreduce.practice.MaxPm25InOneDay.class);

        FileInputFormat.addInputPath(job, new Path("C:\\Users\\WS\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\input\\pm25.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\WS\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\output\\pm"));

        job.setMapperClass(com.mapreduce.practice.MaxPm25InOneDay.WCMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(com.mapreduce.practice.MaxPm25InOneDay.WCReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
