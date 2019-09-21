package com.mapreduce.course.one;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class CourseOne {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {


            String[] lines = value.toString().split(",");
            int sum=0;
            int i=2;
            while (i<lines.length-2){
                sum=sum+Integer.parseInt(lines[i]);
                i++;
            }
            context.write(new Text(lines[0]), new Text(Integer.toString(sum)+"/"+Integer.toString(lines.length-2)));
        }
    }
    public static class MyReducer extends Reducer<Text,Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {


            int count = 0;
            int totalScore = 0;
            int examTimes = 0;
            for (Text value : values) {
                String[] score = value.toString().split("/");
                count++;
                totalScore += Integer.parseInt(score[1]);
                examTimes += Integer.parseInt(score[0]);
            }

            int avg = totalScore/examTimes;

            context.write(key, new Text(count+"\t"+avg));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, CourseOne.class.getSimpleName());
        job.setJarByClass(CourseOne.class);

        FileInputFormat.setInputPaths(job, new Path("/usr/input/CourseOne.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/usr/output/CourseOne") );


        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

    }
}