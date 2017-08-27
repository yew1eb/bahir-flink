package org.apache.flink.streaming.connectors.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.bson.BSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MongoDBBatchExample {

    public static final String INPUT_URI = "mongodb://localhost:27017/test.testData";
    public static final String OUTPUT_URI = "mongodb://localhost:27017/test.outputData";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        JobConf conf = new JobConf();
        MongoConfigUtil.setInputURI(conf, INPUT_URI);
        // create a MongoDBInputFormat, using a Hadoop input format wrapper
        DataSet<Tuple2<BSONWritable, BSONWritable>> input = env.createInput(
                MongoDBUtils.createMongoDBInput(new MongoInputFormat(), BSONWritable.class, BSONWritable.class, conf)
        );


        DataSet<Tuple2<IntWritable, BSONWritable>> output = input.map(
                new MapFunction<Tuple2<BSONWritable, BSONWritable>, Tuple2<IntWritable, BSONWritable>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<IntWritable, BSONWritable> map(Tuple2<BSONWritable, BSONWritable> record) throws Exception {
                        BSONWritable value = record.f1;

                        BSONObject docObj = value.getDoc();
                        int uid = (Integer) docObj.get("uid");
                        String name = (String) docObj.get("name");
                        int age = (Integer) docObj.get("age");
                        BSONObject subDocObj = (BSONObject) docObj.get("feature");
                        List<String> otherInfos = new ArrayList<>();
                        otherInfos.add((String) subDocObj.get("feature0"));
                        otherInfos.add((String) subDocObj.get("feature1"));


                        BasicDBObject outObj = new BasicDBObject();
                        outObj.put("uid", uid);
                        outObj.put("name", name);
                        outObj.put("age", age);

                        BSONWritable bsonWritable = new BSONWritable();
                        bsonWritable.setDoc(outObj);
                        return new Tuple2<>(new IntWritable(uid), bsonWritable);
                    }
                });

        // emit result
        if (!params.has("output")) {
            //MongoConfigUtil.setOutputURI(conf, OUTPUT_URI);
            //output.output(MongoDBUtils.createMongoDBOutput(new MongoOutputFormat<IntWritable, BSONWritable>(), conf));

            JobConf cnf = new JobConf();
            MongoConfigUtil.setOutputURI(cnf, OUTPUT_URI);
            output.output(new HadoopOutputFormat<IntWritable, BSONWritable>(new MongoOutputFormat<IntWritable, BSONWritable>(), cnf));

            // execute program
            env.execute("MongoDB InputFormat Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            output.print();
        }
    }

    public static class UserInfo {
        public int uid;
        public String name;
        public int age;
        public List<String> otherInfos;

        public UserInfo() {
        }

        public UserInfo(int uid, String name, int age, List<String> otherInfos) {
            this.uid = uid;
            this.name = name;
            this.age = age;
            this.otherInfos = otherInfos;
        }

        @Override
        public String toString() {
            return String.format("{uid=%d, name=%s, age=%d, otherInfos=%s}", uid, name, age, otherInfos);
        }
    }
}
