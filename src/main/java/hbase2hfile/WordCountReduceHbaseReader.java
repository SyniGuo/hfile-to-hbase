package hbase2hfile;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wangxufeng on 2014/12/16.
 */
public class WordCountReduceHbaseReader extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            result.set(val);
            context.write(key, result);
        }
    }
}
