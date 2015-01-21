package hfile2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.cfgLoader;

import java.io.IOException;
import java.util.*;

/**
 * Created by wangxufeng on 2014/12/16.
 */
public class WordCountMapperHbase extends Mapper<Object, Text, IntWritable, MapWritable> {
    private static String _logcfg = "/opt/tools/hadoop-2.4.1/etc/hadoop/log.properties";
    private static Properties _prop = new Properties();
    private static cfgLoader _cfgLoader = new cfgLoader();

    private final static Random random = new Random(126);


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        _prop = _cfgLoader.loadConfig(_logcfg);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        MapWritable logSeg = new MapWritable();

        Configuration conf = context.getConfiguration();
        String topic = conf.get("hfile.topic");

        String logKey = "log." + topic;
        String[] logKeySegList = _prop.get(logKey).toString().split("-");

        String[] sentenceSeg = value.toString().split("  ");
        if (2 == sentenceSeg.length) {
            StringTokenizer itr = new StringTokenizer(sentenceSeg[1].toString());
            if (itr.countTokens() == sentenceSeg.length) {
                Integer randomKey = random.nextInt();
                Integer keyOffset = 0;
                while (itr.hasMoreTokens()) {
                    logSeg.put(new Text(logKeySegList[keyOffset]), new Text(itr.nextToken()));
                    keyOffset++;
                }
                context.write(new IntWritable(randomKey), logSeg);
            }
        }
    }
}
