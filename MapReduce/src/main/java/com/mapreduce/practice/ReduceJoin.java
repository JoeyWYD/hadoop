package com.mapreduce.practice;


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

public class ReduceJoin{


    public static class Mapjoin extends Mapper<LongWritable, Text,Text,Text>{
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();

            String path = inputSplit.getPath().toString();

            if (path.contains("pm25")) {
                String []splits = value.toString().split(",");
                if (splits.length == 3){
                    context.write(new Text(splits[1]),new Text("pm"+splits[0]+"_"+splits[2]));
                }
            } else if (path.contains("city")){
                String []splits = value.toString().split(" ");
                if (splits.length == 5){
                    context.write(new Text(splits[0]),new Text("city"+splits[1]+"_"+splits[2]+"_"+splits[3]+"_"+splits[4]));
                }
            }
        }
    }

    public static class Reducejoin extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> pm = new ArrayList<String>();
            ArrayList<String> city = new ArrayList<String>();

            for (Text value : values){
                String line=value.toString();
                if (line.startsWith("pm"))
                    pm.add(line.substring(2));
                else if (line.startsWith("city"))
                    city.add(line.substring(4));
            }

            for (String s1:pm)
                for (String s2 :city)
                    context.write(key,new Text(s1+"_"+s2));


        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,ReduceJoin.class.getSimpleName());

        job.setJarByClass(com.mapreduce.practice.ReduceJoin.class);

        FileInputFormat.addInputPath(job,new Path("C:\\Users\\WS\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\input\\city_air.txt"));
        FileInputFormat.addInputPath(job,new Path("C:\\Users\\WS\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\input\\pm25.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\WS\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\output\\pm25"));

        job.setMapperClass(com.mapreduce.learn.ReduceJoin.Map_Rjoin.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(com.mapreduce.learn.ReduceJoin.Reduce_Rjoin.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }


}