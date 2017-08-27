package org.apache.flink.streaming.connectors.mongodb;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * @author zhouhai
 * @createTime 2017/8/27
 * @description
 */
public class MongoSink extends RichSinkFunction<Document> {
    public static String CollectionName = "collection-a";
    private static Mongo mongo = null;
    private static MongoClient mongoClient = null;

    /**
     * 初始化连接池
     */
    private static void initDBPrompties() {
        try {

            MongoClientOptions.Builder mcob = MongoClientOptions.builder();
            mcob.connectionsPerHost(1000);
            mcob.socketKeepAlive(true);
            mcob.readPreference(ReadPreference.secondaryPreferred());
            MongoClientOptions mco = mcob.build();

            mongoClient = new MongoClient(Arrays.asList(
                    new ServerAddress("127.0.0.1", 27017),
                    new ServerAddress("127.0.0.1", 27017),
                    new ServerAddress("127.0.0.1", 27017)), mco);


            mongo = new Mongo("host", "port");
            MongoOptions opt = mongo.getMongoOptions();
            opt.connectionsPerHost = 11;//"POOLSIZE";
            opt.threadsAllowedToBlockForConnectionMultiplier = 23;//"BLOCKSIZE";
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(String t) {
        try {
            MongoDatabase mongo = mongo.getDB(dbName);
            MongoCollection coll = mongo.getCollection(CollectionName);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open(Configuration config) {
        mongoService = new MongoService();
        try {
            super.open(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(Document document) throws Exception {

    }
}
