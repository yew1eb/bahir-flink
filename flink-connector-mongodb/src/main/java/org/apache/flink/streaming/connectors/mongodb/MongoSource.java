package org.apache.flink.streaming.connectors.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.bson.Document;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Note: Checkpoint File stores the state information that flink uses to track where it was last reading
 */
public class MongoSource extends RichSourceFunction<Document> {

    private String inputClientURI;
    private String mongoDatabase;
    private String mongoCollection;
    private String checkPointFile;
    private String documentId;
    private boolean isRunning;

    public MongoSource(Properties property) {

        this.inputClientURI = property.getProperty("mongo.input.uri");
        this.mongoDatabase = property.getProperty("inputDatabase");
        this.mongoCollection = property.getProperty("collection");
        this.checkPointFile = property.getProperty("checkPointFile");
        this.documentId = "";
        this.isRunning = true;

    }

    @Override
    public void run(SourceContext<Document> context) throws Exception {

        //Getting connection to transaction database
        List<ServerAddress> seeds = new ArrayList<ServerAddress>();
        String[] hosts = this.inputClientURI.split(",");

        for (int i = 0; i < hosts.length; i++) {
            seeds.add(new ServerAddress(hosts[i]));
        }

        MongoCollection<Document> coll = new MongoClient(seeds).getDatabase(this.mongoDatabase)
                .getCollection(this.mongoCollection);

        BufferedReader inputReader = new BufferedReader(new FileReader(this.checkPointFile));
        if ((this.documentId = inputReader.readLine()) != null) {
            this.documentId = this.documentId.split(":")[1];
        }

        Document doc = new Document();
        MongoCursor<Document> cursor;
        while (this.isRunning) {

            Document query = new Document();

            if (!this.documentId.equals("")) {
                query.put("_id", new Document("$gt", this.documentId));
            }

            cursor = coll.find(query).iterator();
            while (cursor.hasNext()) {
                doc = cursor.next();
                context.collect(doc);
            }

            if (doc.containsKey("_id")) {
                this.documentId = doc.get("_id").toString();
            }
        }
    }

    @Override
    public void cancel() {

        try {
            FileWriter outputWriter = new FileWriter(new File(this.checkPointFile));
            outputWriter.write(this.mongoCollection + ":" + documentId);

            this.isRunning = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
