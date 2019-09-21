package HTH;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Hbasetohdfs {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建连接配置
        String zoo ="master:2181,node1:2181,node2:2181";
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum",zoo);
        //创建job
        Job job = Job.getInstance(conf, Hbasetohdfs.class.getSimpleName());
        //指定jar包
        job.setJarByClass(Hbasetohdfs.class);

        //创建扫描对象
        Scan scan = new Scan();
        scan.addFamily("info".getBytes());
        //初始化map类
        TableMapReduceUtil.initTableMapperJob("student",scan, ToHdfsMapper.class, NullWritable.class, Text.class,job);
        //指定输出目录
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.waitForCompletion(true);
    }
}
