package hfile2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by wangxufeng on 2014/12/16.
 */
public class MapReduceHbaseDriver {
    public static void main(String[] args) throws Exception {
        String tablename = "wordcount";

        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "ResourceManager, DataNode1, DataNode2");
//        conf.set("hbase.zookeeper.property.clientPort","2181");
//        conf.set("hbase.master", "ResourceManager:60000");

        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tablename)) {
            System.out.println("table exists! recreating ...");
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
        HTableDescriptor htd = new HTableDescriptor(tablename);
        HColumnDescriptor hcd = new HColumnDescriptor("content");
        htd.addFamily(hcd);
        admin.createTable(htd);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: wordcount <in> <out>" + otherArgs.length);
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MapReduceHbaseDriver.class);
        job.setMapperClass(WordCountMapperHbase.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TableMapReduceUtil.initTableReducerJob(tablename, WordCountReducerHbase.class, job);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
