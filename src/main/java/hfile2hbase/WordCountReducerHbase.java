package hfile2hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;

import java.io.IOException;

/**
 * Created by wangxufeng on 2014/12/16.
 */
public class WordCountReducerHbase extends TableReducer<Text, MapWritable, ImmutableBytesWritable> {

    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException,
            InterruptedException {
        Put put = new Put(key.getBytes());

        for (MapWritable logSeg : values) {
            for (Writable logKey: logSeg.keySet()) {
                put.add(Bytes.toBytes("default"), Bytes.toBytes(logKey.toString()), Bytes.toBytes(logSeg.get(logKey)
                        .toString()));
            }
        }
        context.write(new ImmutableBytesWritable(key.getBytes()), put);
    }
}
