package org.apache.flink.batch.connectors.mongodb;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * @author zhouhai
 * @createTime 2017/8/27
 * @description
 */
public final class MongoDBUtils {


    public static <K, V> MongoDBInputFormat<K, V> createMongoDBInput(InputFormat<K, V> mapredInputFormat, Class<K> key, Class<V> value, JobConf job) {
        return new MongoDBInputFormat(mapredInputFormat, key, value, job);
    }

    public static <KK, VV> MongoDBOutputFormat<KK, VV> createMongoDBOutput(OutputFormat<KK, VV> mapredOutputFormat, JobConf job) {
        return new MongoDBOutputFormat<KK, VV>(mapredOutputFormat, job);
    }

}
