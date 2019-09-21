package com.mapreduce.practice;


import com.mapreduce.learn.ContentCount;
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



public class ArticalCount {

    /**
     *  hdfs dfs -rm -r /usr/input/A_Greek_to_Remember
     *  hdfs dfs -put A_Greek_to_Remember.txt /usr/input/
     *  hdfs dfs -rm -r /usr/input/Run.txt
     *  hdfs dfs -put Run.txt /usr/input/
     *  hdfs dfs -rm -r /usr/output/A_Greek_to_Remember
     *  hdfs dfs -rm -r /usr/output/Run
     *  hadoop jar MapReduce-1.0-SNAPSHOT.jar com.mapreduce.practice.ArticalCount /usr/input/A_Greek_to_Remember.txt /usr/output/A_Greek_to_Remember
     *  hadoop jar MapReduce-1.0-SNAPSHOT.jar com.mapreduce.practice.ArticalCount /usr/input/Run.txt /usr/output/Run
     *  hdfs dfs -cat /usr/output/A_Greek_to_Remember/part-r-00000
     *  hdfs dfs -cat /usr/output/Run/part-r-00000
     */




    public static class WCMap extends Mapper<LongWritable, Text,Text, IntWritable> {
        protected void map(LongWritable key , Text value ,Context context) throws IOException, InterruptedException {
            String line = value.toString();

            /**
             *             line = line.replace("!","");
             *             line = line.replace(",","");
             *             line = line.replace(".","");
             *             line = line.replace("\"","");
             *             line = line.replace("!","");
             *             line = line.replace(":","");
             *
             */

            line=line.replaceAll("!|,|.|\\|:|;"," ");
            String [] words= line.split(" ");
            for (String word :words){
                context.write(new Text(word),new IntWritable(1));
                context.write(new Text("_sum_"),new IntWritable(1));

            }
        }
    }
    public static class WCReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void text(String name) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, ContentCount.class.getSimpleName());
        job.setJarByClass(ContentCount.class);

        FileInputFormat.addInputPath(job, new Path("C:\\Users\\WS\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\input\\"+name+".txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\WS\\IdeaProjects\\Hadoop\\MapReduce\\src\\data\\output\\"+name));

        job.setMapperClass(ContentCount.ConMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(ContentCount.ConReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        text("A_Greek_to_Remember");
        text("Run");
    }

}
