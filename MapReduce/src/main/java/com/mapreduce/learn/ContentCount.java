package com.mapreduce.learn;

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

public class ContentCount {






    public static class ConMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //将句子中的标点符号替代为空字符
            String[] symbols = {",", ".", "?", ":", "\"", "!"};
            for (String symbol : symbols) {
                line = line.replace(symbol,"");
            }
            String[] words = line.split(" ");
            for (String word : words) {
                if(!word.equals("")) {
                    context.write(new Text(word), new IntWritable(1));
                    context.write(new Text("_flag_"), new IntWritable(1));
                }
            }
        }
    }

    public static class ConReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String inkey = key.toString();
            int allSum = 0, sum = 0;
            if(inkey.equals("_flag_")){
                for (IntWritable value : values) {
                    allSum += value.get();
                }
                context.write(new Text("allwords"), new IntWritable(allSum));
            }else {
                for (IntWritable value : values) {
                    sum += value.get();
                }
                context.write(key, new IntWritable(sum));
            }
        }
    }




    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, ContentCount.class.getSimpleName());
        job.setJarByClass(ContentCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ConMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(ConReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
