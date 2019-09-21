//package com.mapreduce.Text1;
//
//import java.io.IOException;
//import java.util.StringTokenizer;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//
//public class mapreduce3_1 {
//    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
//
//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
//
//        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString()," ,.\":\t\n");
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken().toLowerCase());
//                context.write(word, one);
//            }
//        }
//    }
//
//
//
//    public static class IntSumReducer
//            extends Reducer<Text,IntWritable,Text,IntWritable> {
//        private IntWritable result = new IntWritable();
//
//        public void reduce(Text key, Iterable<IntWritable> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key, result);
//
//        }
//    }
//
//    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
//
//        public int compare(WritableComparable a, WritableComparable b) {
//            return -super.compare(a, b);
//        }
//        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//            return -super.compare(b1, s1, l1, b2, s2, l2);
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//
//        Path tempDir = new Path("wordcount-temp-output");
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: wordcount <in> [<in>...] <out>");
//            System.exit(2);
//        }
//
//
//        Job job = Job.getInstance(conf, mapreduce3_1.class.getSimpleName());
//        job.setJarByClass(mapreduce3_1.class);
//
//        job.setMapperClass(mapreduce3_1.WCMap.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setReducerClass(mapreduce3_1.WCReduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//        FileInputFormat.addInputPath(job, new Path("/data/input/dns_log.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("/data/output/dns_log"));
//
//
//        job.waitForCompletion(true);
//
//        Job sortjob = new Job(conf, "sort");
//        FileInputFormat.addInputPath(sortjob, tempDir);
//        sortjob.setInputFormatClass(SequenceFileInputFormat.class);
//        sortjob.setMapperClass(InverseMapper.class);
//        sortjob.setNumReduceTasks(1);
//        FileOutputFormat.setOutputPath(sortjob, new Path(otherArgs[otherArgs.length - 1]));
//        sortjob.setOutputKeyClass(IntWritable.class);
//        sortjob.setOutputValueClass(Text.class);
//        sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);
//
//        sortjob.waitForCompletion(true);
//
//
//
//        Configuration conf = new Configuration();
//
//
//
//        job.waitForCompletion(true);
//
//    }
//
//}