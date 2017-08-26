package org.apache.flink.batch.connectors.mongodb;

import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * @author zhouhai
 * @createTime 2017/8/27
 * @description
 */
public class MongoDBOutputFormat<K, V> extends HadoopOutputFormat<K, V> {

    public MongoDBOutputFormat(OutputFormat<K, V> mapredOutputFormat, JobConf job) {
        super(mapredOutputFormat, job);
    }

}
