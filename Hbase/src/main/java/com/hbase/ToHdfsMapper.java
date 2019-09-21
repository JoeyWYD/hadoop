package HTH;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ToHdfsMapper extends TableMapper<NullWritable, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取rowkey也就是id
        String id = Bytes.toString(key.get());
        //从result中获取列值
        String name = Bytes.toString(value.getValue("info".getBytes(),"name".getBytes()));
        Integer age = Bytes.toInt(value.getValue("info".getBytes(),"age".getBytes()));
        String gender = Bytes.toString(value.getValue("info".getBytes(),"gender".getBytes()));
        String clazz = Bytes.toString(value.getValue("info".getBytes(),"clazz".getBytes()));

        //拼接各值
        String line = id+","+name+","+age.toString()+","+gender+","+clazz;
        context.write(NullWritable.get(), new Text(line));

    }
}
