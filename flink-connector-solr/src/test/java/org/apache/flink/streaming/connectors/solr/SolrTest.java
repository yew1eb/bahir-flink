/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.solr;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;

/**
 * Created by poj871 on 11/2/15.
 */
public class SolrTest extends SolrJettyTestBase {

    private static final int NUM_ELEMENTS = 20;
    public static MiniSolrCloudCluster cluster;
    protected static CloudSolrClient cloudSolrClient;
    public static File dataDir;
    public static String zkHost;
    public static File workingDir;


    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();


    @BeforeClass
    public static void setupSolrCluster() throws Exception{

        dataDir = tempFolder.newFolder();
        File solrXml = new File("src/test/resources/solr.xml");

        try {
            cluster = new MiniSolrCloudCluster(1, null, workingDir, solrXml, null, null);
        }

        catch (Exception e){
            e.printStackTrace();
        }

        cloudSolrClient = new CloudSolrClient(cluster.getZkServer().getZkAddress(), true);
        cloudSolrClient.connect();
        assertTrue(!cloudSolrClient.getZkStateReader().getClusterState().getLiveNodes().isEmpty());
        zkHost = cluster.getZkServer().getZkHost();


    }


    @Test
    public void testSolrCloud() throws Exception{

        HttpSolrClient solrClient = (HttpSolrClient) getSolrClient();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());
        Map<String, String> config = Maps.newHashMap();

        config.put("solr_type","Http");
        config.put("solr_location",solrClient.getBaseURL());
        config.put("zkconnectionstring", zkHost);

        source.addSink(new SolrSink<Tuple2<Integer, String>>(config, new TestSolrInputDocBuilder()));
        env.execute("Solr Test Case");


    }


    private static class TestSourceFunction implements SourceFunction<Tuple2<Integer, String>> {

        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;


        public void run(SourceContext<Tuple2<Integer, String>> sourceContext) throws Exception {

            for (int i = 0; i < NUM_ELEMENTS && running; i++){
                sourceContext.collect(Tuple2.of(i, "message #" + i));
            }
        }

        public void cancel() {
            running = false;
        }
    }

    private static class TestSolrInputDocBuilder implements SolrInputDocumentBuilder<Tuple2<Integer, String>> {

        private static final long serialVersionUID = 1L;


        public SolrInputDocument createSolrInputDocument(Tuple2<Integer, String> element, RuntimeContext ctx) {

            SolrInputDocument document = new SolrInputDocument();
            document.addField("data", element.f1);
            document.addField("id", element.f0);

            return document;
        }
    }

}
