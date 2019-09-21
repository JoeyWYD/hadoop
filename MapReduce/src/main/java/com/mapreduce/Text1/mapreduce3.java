package com.mapreduce.Text1;

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

public class mapreduce3 {


    public static class WCMap extends Mapper<LongWritable, Text, Text, Text>{

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String [] words = line.split("\\|");
            String str="";
            if (words.length>=5){
                for (int i=0;i<words.length;i++) if (i!=1) str=str+"|"+words[i];
            }
            context.write(new Text(words[1]),new Text(str.substring(1)));
        }
    }

    public static class WCReduce extends Reducer<Text, Text, Text, Text>{

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String k;
            String v;
            String [] r;
            String rr;
            if (key.toString().equals("www.taobao.com"))
                k = "ShoppingAction";
            else
                k=key.toString();

            for (Text value :values){
                r=value.toString().split("\\|");
                rr=r[0]+"|"+k;
                for (int i=1;i<r.length;i++) rr=rr+"|"+r[i];
                context.write(new Text(k), new Text(rr));
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, mapreduce3.class.getSimpleName());
        job.setJarByClass(mapreduce3.class);

        FileInputFormat.addInputPath(job, new Path("/data/input/dns_log.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/data/output/dns_log"));

        job.setMapperClass(mapreduce3.WCMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(mapreduce3.WCReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }



}
