package com.mapreduce.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AverageWage {


    public static class AvgMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //以空格切分
            String[] splits = value.toString().split(" ");
            if (splits.length == 3) {
                //姓名为key
                String mkey = splits[0];
                //工资作为value，并转化为Int类型
                int mvalue = Integer.parseInt(splits[2]);

                context.write(new Text(mkey), new IntWritable(mvalue));
            }
        }
    }
    public static class AvgReducer extends Reducer<Text, IntWritable, Text, FloatWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //用来求总工资
            int sum = 0;
            //用来统计拿工资的月数
            int count = 0;
            //遍历values求总工资并对月数进行统计
            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }
            //求平均工资
            float avg = sum/count;
            context.write(key, new FloatWritable(avg));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置文件
        Configuration conf = new Configuration();
        //创建任务
        Job job = Job.getInstance(conf, AverageWage.class.getSimpleName());
        //指定jar包
        job.setJarByClass(AverageWage.class);
        //指定输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //指定map类
        job.setMapperClass(AvgMapper.class);
        //指定map输出键值的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定reduce类
        job.setReducerClass(AvgReducer.class);
        //指定reduce输出键值的数据类型的类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);


        //提交任务
        job.waitForCompletion(true);
    }

}
