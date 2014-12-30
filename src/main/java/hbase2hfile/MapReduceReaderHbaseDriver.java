package hbase2hfile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * Created by wangxufeng on 2014/12/16.
 */
public class MapReduceReaderHbaseDriver {
    public static void main(String[] args) throws Exception {
        String tablename = "wordcount";

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>, current args length: " + otherArgs.length);
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MapReduceReaderHbaseDriver.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setReducerClass(WordCountReduceHbaseReader.class);

        Scan scan = new Scan(args[0].getBytes());
        TableMapReduceUtil.initTableMapperJob(tablename, scan, WordCountMapperHbaseReader.class, Text.class, Text.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
