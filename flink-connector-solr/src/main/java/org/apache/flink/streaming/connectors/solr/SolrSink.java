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


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.LoggerFactory;

import java.util.Map;
import org.slf4j.Logger;

public class SolrSink<T> extends RichSinkFunction<T> {

    private final Logger LOG = LoggerFactory.getLogger(SolrSink.class);
    public static final String CONFIG_KEY_SOLR_TYPE = "solr_type";
    public static final String SOLR_LOCATION = "solr_location";
    public static final String SOLR_ZK_STRING = "zkconnectionstring";
    private final Map<String, String> userConfig;
    private final SolrInputDocumentBuilder solrInputDocumentBuilder;

    private transient SolrClient solrClient;

    public SolrSink(Map<String, String> userConfig, SolrInputDocumentBuilder solrInputDocumentBuilder){
        this.userConfig = userConfig;
        this.solrInputDocumentBuilder = solrInputDocumentBuilder;
    }

    @Override
    public void open(Configuration configuration) {

        solrClient = createClient(userConfig.get(CONFIG_KEY_SOLR_TYPE));
    }

    @Override
    public void invoke(T t) throws Exception {

        SolrInputDocument solrInputDocument = solrInputDocumentBuilder.createSolrInputDocument(t, getRuntimeContext());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Emitting Solr Input Doc: {}", solrInputDocument);
        }
        System.out.println(solrInputDocument);
        solrClient.add(solrInputDocument);

    }

    @Override
    public void close() {

    }


    public SolrClient createClient(String solrType) {
        if (solrType.equals("Http"))
            return new HttpSolrClient(userConfig.get(SOLR_LOCATION));
        else if (solrType.equals("Cloud"))
            return new CloudSolrClient(userConfig.get(SOLR_ZK_STRING));
        else{
            if (LOG.isInfoEnabled()) {
                LOG.error("Invalid Solr type in the configuration");
            }
            throw new RuntimeException("Invalid Solr type in the configuration, valid types are standard or cloud");
        }
    }
}