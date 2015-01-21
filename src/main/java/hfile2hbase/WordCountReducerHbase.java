package hfile2hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by wangxufeng on 2014/12/16.
 */
public class WordCountReducerHbase extends TableReducer<IntWritable, MapWritable, ImmutableBytesWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<MapWritable> values, Context context) throws IOException,
            InterruptedException {

        int sum = 0;
        for (MapWritable val : values) {

        }
        result.set(sum);
        Put put = new Put(key.getBytes());
        put.add(Bytes.toBytes("default"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
        context.write(new ImmutableBytesWritable(key.getBytes()), put);
    }
}
