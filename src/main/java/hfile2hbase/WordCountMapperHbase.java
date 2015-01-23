package hfile2hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.cfgLoader;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by wangxufeng on 2014/12/16.
 */
public class WordCountMapperHbase extends Mapper<Object, Text, Text, MapWritable> {
    public static final Log LOG = LogFactory.getLog(WordCountMapperHbase.class);

    private static String _logcfg = "/opt/tools/hadoop-2.4.1/etc/hadoop/log.properties";
    private static Properties _prop = new Properties();
    private static cfgLoader _cfgLoader = new cfgLoader();

    private final static SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

    private static Random random = new Random();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        _prop = _cfgLoader.loadConfig(_logcfg, true);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        MapWritable logSeg = new MapWritable();

        Configuration conf = context.getConfiguration();
        String topic = conf.get("hfile.topic");

        String logKey = "log." + topic;
        String[] logKeySegList = _prop.get(logKey).toString().split("\\|");

        StringTokenizer mainItr = new StringTokenizer(value.toString());
        String logStr = "";
        while (mainItr.hasMoreTokens()) {
            String curToken = mainItr.nextToken();
            if (util.common.isNumeric(curToken)) {
                continue;
            } else {
                logStr += curToken;
            }
        }


        //StringTokenizer itr = new StringTokenizer(curToken, "|");
        String[] itr = logStr.split("\\|");
LOG.info("+++++++++++++" + logStr + "---------");
        if (itr.length == logKeySegList.length) {
            Integer randomKey = random.nextInt(65535) % (55536) + 10000;
            Integer keyOffset = 0;
            for (String itrToken : itr) {
                logSeg.put(new Text(logKeySegList[keyOffset]), new Text(itrToken));
                keyOffset++;
            }
            Long curTs = System.currentTimeMillis();
            Long curDayts = 0L;
            String curDay = format.format(curTs);
            try {
                Date curDayDate = format.parse(curDay);
                curDayts = curDayDate.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Long tsDiff = curTs - curDayts;
            String tsDiffStr = tsDiff.toString();
            if (tsDiff.toString().length() >= 8) {
                tsDiffStr = tsDiff.toString().substring(0, 8);
            }
            String hbaseRowKey = randomKey.toString() + tsDiffStr;
LOG.info("+=+=+=+=+=+=" + hbaseRowKey);
            context.write(new Text(hbaseRowKey), logSeg);
        }

    }
}
