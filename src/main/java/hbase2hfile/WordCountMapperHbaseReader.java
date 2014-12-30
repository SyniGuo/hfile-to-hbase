package hbase2hfile;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by wangxufeng on 2014/12/16.
 */
public class WordCountMapperHbaseReader extends TableMapper<Text, Text> {
    public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer("");
        for(java.util.Map.Entry<byte[], byte[]> value : values.getFamilyMap("content".getBytes()).entrySet()) {
            String str = new String(value.getValue());

            if (str != null) {
                sb.append(new String(value.getKey()));
                sb.append(":");
                sb.append(str);
            }
            context.write(new Text(row.get()), new Text(new String(sb)));
        }
    }
}
