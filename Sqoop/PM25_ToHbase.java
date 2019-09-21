package pm25;

import com.shujia.HdfsToHbase2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class PM25_ToHbase {
    public static class PM25Mapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //rowkey为2018_address
            String address = value.toString().split(",")[1];
            String rowskey = "2018_"+address;


            context.write(new Text(rowskey), value);
        }
    }

    public static class PM25Reducer extends TableReducer<Text, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //获取rowkey
            String rowkey = key.toString();
            //创建Put对象
            Put put = new Put(rowkey.getBytes());
            //获取各数据
            for (Text value : values) {
                String[] str = value.toString().split(",");
                if (str.length == 4) {
                    String id = str[0];
                    String address = str[1];
                    String city = str[2];
                    String avg_pm25 = str[3];

                    put.add("info".getBytes(),"id".getBytes(),id.getBytes());
                    put.add("info".getBytes(),"address".getBytes(),address.getBytes());
                    put.add("info".getBytes(),"city".getBytes(),city.getBytes());
                    put.add("info".getBytes(),"avg_pm25".getBytes(),avg_pm25.getBytes());

                    context.write(NullWritable.get(), put);
                }

            }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        //创建连接配置
        String zoo ="master:2181,node1:2181,node2:2181";
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum",zoo);
        //创建job
        Job job = Job.getInstance(conf, PM25_ToHbase.class.getSimpleName());
        //指定jar包
        job.setJarByClass(PM25_ToHbase.class);
        //指定mapper类，以及Mapper的输出键值类型
        job.setMapperClass(PM25Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定输入路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //指定reducer
        TableMapReduceUtil.initTableReducerJob("city_avg_pm25", PM25Reducer.class, job);
        //指定reduce输出键值
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        job.waitForCompletion(true);
    }


}
