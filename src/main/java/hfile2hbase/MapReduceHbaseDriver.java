package hfile2hbase;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ZkUtils;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by wangxufeng on 2014/12/16.
 */
public class MapReduceHbaseDriver extends Configured implements Tool{
    static Logger LOG = LoggerFactory.getLogger(MapReduceHbaseDriver.class);

    //public static void main(String[] args) throws Exception {
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        CommandLineParser parser = new PosixParser();
        Options options = buildOptions();

        CommandLine cmd = parser.parse(options, args);
        String hfileDir = cmd.getOptionValue("hfile-dir", "/temp");
        conf.set("hfile.path", hfileDir);
        String specday = cmd.getOptionValue("day", "");
        conf.set("exec.day", specday);
        conf.set("hbase.zookeeper.quorum", cmd.getOptionValue("zk-connect", "localhost:2182"));
//        conf.set("hbase.zookeeper.property.clientPort","2181");
//        conf.set("hbase.master", "ResourceManager:60000");

        Long curTs = System.currentTimeMillis();

//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 1) {
//            System.err.println("Usage: wordcount <in> <out>" + otherArgs.length);
//            System.exit(2);
//        }


        List<String> dayList = new ArrayList<String>();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        if (specday.isEmpty()) {
            // include former day dir to avoid crossday problem
            Long yesterdayTs = curTs - 24 * 3600 * 1000;
            String yesterday = format.format(yesterdayTs);
            dayList.add(yesterday);
            String today = format.format(curTs);
            dayList.add(today);
        } else {
            Date date = format.parse(specday);
            String day = format.format(date.getTime());
            dayList.add(day);
        }

        // iterate the day list to get the associative topic file list
        Map<String, Map<Long, String>> topicFileAsso = new HashMap<String, Map<Long, String>>();

        for (String thisday : dayList) {
            // To get the file list from the hdfs_directory
            String hfilePath = hfileDir + '/' + thisday;
            FileSystem hdfs = FileSystem.get(URI.create(hfilePath), conf);
            if (hdfs.exists(new Path(hfilePath))) {
                FileStatus[] fstat = hdfs.listStatus(new Path(hfilePath));
                Path[] listPath = FileUtil.stat2Paths(fstat);
                for (Path p : listPath) {
                    String[] pathSeg = p.toString().split("/");
                    String pathFile = pathSeg[pathSeg.length - 1];
                    String[] fileSeg = pathFile.split("-");
                    if (fileSeg.length >= 6) {
                        String topic = fileSeg[0];
                        String fileTs = fileSeg[1];
                        String mapOrReduce = fileSeg[5];
                        if ("r" == mapOrReduce) {
                            LOG.info("THIS IS REDUCE RESULT: {}", mapOrReduce);
                        } else {
                            LOG.info("THIS IS DIRECTLY MAP RESULT: {}", mapOrReduce);
                        }
System.out.println("============= " + topic + " ----------- " + fileTs + " +++++++++++ " + mapOrReduce);

                        Map<String, Long> tsMap = getZkTsInfo(conf, topic);
                        //Long curReadHfileTs = tsMap.get("curReadHfileTs");
                        Long lastReadHfileTs = tsMap.get("lastReadHfileTs");
System.out.println("------------- " + lastReadHfileTs);
                        Long fileTsLong = Long.parseLong(fileTs);
System.out.println("------------- " + fileTsLong);
                        if ((fileTsLong > lastReadHfileTs) || !specday.isEmpty()) {
                            String tablename = topic;

                            HBaseAdmin admin = new HBaseAdmin(conf);
                            if (admin.tableExists(tablename)) {
                                LOG.info("table exists! IGNORING ...");
                                //                            admin.disableTable(tablename);
                                //                            admin.deleteTable(tablename);
                            } else {
                                HTableDescriptor htd = new HTableDescriptor(tablename);
                                HColumnDescriptor hcd = new HColumnDescriptor("default");
                                htd.addFamily(hcd);
                                admin.createTable(htd);
                            }

                            String curHDFSFile = hfilePath + '/' + pathFile;
                            Map<Long, String> topicFileList = new HashMap<Long, String>();
                            if (topicFileAsso.containsKey(topic)) {
                                topicFileList = topicFileAsso.get(topic);
                            }
                            topicFileList.put(fileTsLong, curHDFSFile);
                            topicFileAsso.put(topic, topicFileList);
                        }
                    }
                }
            } else {
                LOG.info("HDFS file directory not exist: {}", hfilePath);
            }
        }


        Boolean overall_success = true;

        // Iterate topicFileAsso to execute importing process (from hdfs to hbase)
        for (String topic: topicFileAsso.keySet()) {
            Long lasthfileTs = 0L;
            Map<Long, String> topicFileList = topicFileAsso.get(topic);

            Job job = Job.getInstance(conf, "hfile2hbase_" + topic);
            job.setJarByClass(MapReduceHbaseDriver.class);
            job.setMapperClass(WordCountMapperHbase.class);
            for (Long fileTsLong: topicFileList.keySet()) {
                FileInputFormat.addInputPath(job, new Path(topicFileList.get(fileTsLong)));
                lasthfileTs = fileTsLong > lasthfileTs ? fileTsLong : lasthfileTs;
            }
            TableMapReduceUtil.initTableReducerJob(topic, WordCountReducerHbase.class, job);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            boolean success = job.waitForCompletion(true);
            if (success) {
                commitZkTsInfo(conf, topic, lasthfileTs);
            } else {
                overall_success = false;
            }
        }

        return overall_success ? 0: -1;
    }

    @SuppressWarnings("static-access")
    private Options buildOptions() {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("dir")
                .withLongOpt("hfile-dir")
                .hasArg()
                .withDescription("hfile dir")
                .create("p"));
        options.addOption(OptionBuilder.withArgName("")
                .withLongOpt("day")
                .hasArg()
                .withDescription("specific day - WARNING! add this param may cause data duplication! Use in Caution!")
                .create("d"));
        options.addOption(OptionBuilder.withArgName("zk")
                .withLongOpt("zk-connect")
                .hasArg()
                .withDescription("ZooKeeper connection String")
                .create("z"));

        return options;
    }

    /**
     * 获取hdfs文件导入状态信息（时间戳）
     * @param conf
     * @param topic
     * @return
     * @throws IOException
     */
    private Map getZkTsInfo(Configuration conf, String topic) throws IOException {
        Map<String, Long> tsMap = new HashMap<String, Long>();
        Long curReadHfileTs = 0L;
        Long lastReadHfileTs = 0L;
        ZkUtils zk = new ZkUtils(conf);
        try {
            curReadHfileTs = zk.getCurReadHfileTs(topic);
            lastReadHfileTs = zk.getLastReadHfileTs(topic);
        } catch (Exception e) {
            rollback("getZkTsInfo ERROR!");
        } finally {
            zk.close();
        }
        tsMap.put("curReadHfileTs", curReadHfileTs);
        tsMap.put("lastReadHfileTs", lastReadHfileTs);
        return tsMap;
    }

    private void rollback(String errorInfo) {
        LOG.error("{}", errorInfo);
    }

    /**
     * 提交hdfs文件导入状态信息（时间戳）
     * @param conf
     * @param topic
     * @param lasthfileTs
     * @throws IOException
     */
    private void commitZkTsInfo(Configuration conf, String topic, Long lasthfileTs) throws IOException {
        ZkUtils zk = new ZkUtils(conf);
        try {
            zk.setLastReadHfileTs(topic, lasthfileTs);
        } catch (Exception e) {
            rollback("commitZkTsInfo ERROR!");
        } finally {
            zk.close();
        }
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MapReduceHbaseDriver(), args);
        System.exit(exitCode);
    }
}
