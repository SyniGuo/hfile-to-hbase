package util;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by wangxufeng on 2014/9/11.
 */
public class ZkUtils implements Closeable {
    private static Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

    private static final String LAST_HFILE_STAT_PATH = "/hfile2hbase/lasthfile/stat";
    private static final String LAST_HFILE_TS_PATH = "/file2hbase/lasthfile/ts";

    private static final String CUR_HFILE_TS_PATH = "/kafka2hfile/curhfile/ts";


    /**
     * zookeeper客户端对象
     */
    private ZkClient client;
    /**
     * kafka brokers列表
     */
    Map<String, String> brokers;


    public ZkUtils(Configuration config) {
        connect(config);
    }

    /**
     * 连接zookeeper
     * @param config
     */
    private void connect(Configuration config) {
        String zk = config.get("hbase.zookeeper.quorum");
        int stimeout = config.getInt("zk.sessiontimeout.ms", 10000);
        int ctimeout = config.getInt("zk.connectiontimeout.ms", 10000);

        client = new ZkClient(zk, stimeout, ctimeout, new StringSerializer() );
    }

    /**
     * 获取上次导入的hdfs文件信息
     * @param topic
     * @return
     */
    public Map getLastReadHfileStat(String topic) {
        Map<String, String> lastHfileStat = new HashMap<String, String>();

        String path = LAST_HFILE_STAT_PATH + "/" + topic;
        if (!client.exists(path)) {
            client.createPersistent(path, true);
            return lastHfileStat;
        }
        String data = client.readData(LAST_HFILE_STAT_PATH + "/" + topic);
        LOG.info("--Last Hfile Status: topic: " + topic + " stat: " + data);
        JSONObject jsonObject = new JSONObject(data);
        String lastts = jsonObject.getString("lastts");

        lastHfileStat.put(topic, lastts);
        return lastHfileStat;
    }

    /**
     * 获取上次导入hbase的hdfs文件时间戳信息（用于定位导入进度）
     * @param topic
     * @return timestamp
     */
    public Long getLastReadHfileTs(String topic) {
        Long lastHfileTs = 0L;

        String path = LAST_HFILE_TS_PATH + '/' + topic;
        if (!client.exists(path)) {
            //client.createPersistent(path, true);
            return lastHfileTs;
        }
        String data = client.readData(path);
        LOG.info("--Last Hfile Ts: topic: " + topic + " timestamp: " + data);
        lastHfileTs = Long.parseLong(data);
        return lastHfileTs;
    }

    /**
     * 设置本次导入的hdfs文件时间戳信息（用于定位导入进度）
     * @param topic
     * @param ts
     */
    public boolean setLastReadHfileTs(String topic, Long ts) {
        String path = LAST_HFILE_TS_PATH + '/' + topic;
        if (!client.exists(path)) {
            client.createPersistent(path, true);
        }
        client.writeData(path, ts);
        return true;
    }


    /**
     * 获取上次导入hdfs的时间戳信息
     * @param topic
     * @return timestamp
     */
    public Long getCurReadHfileTs(String topic) {
        Long curHfileTs = 0L;

        String path = CUR_HFILE_TS_PATH + '/' + topic;
        if (!client.exists(path)) {
            //client.createPersistent(path, true);
            return curHfileTs;
        }
        String data = client.readData(path);
        LOG.info("--current Hfile Ts: topic: " + topic + " timestamp: " + data);
        curHfileTs = Long.parseLong(data);
        return curHfileTs;
    }



    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }



    static class StringSerializer implements ZkSerializer {
        public StringSerializer() {}
        @Override
        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null) return null;
            return new String(data);
        }

        @Override
        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }

    }

}
