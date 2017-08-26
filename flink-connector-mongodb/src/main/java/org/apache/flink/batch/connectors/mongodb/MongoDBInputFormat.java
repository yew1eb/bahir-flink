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

package org.apache.flink.batch.connectors.mongodb;

import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author zhouhai
 * @createTime 2017/8/26
 * @description
 */
public class MongoDBInputFormat<K, V> extends HadoopInputFormat<K, V> {

    public MongoDBInputFormat(InputFormat<K, V> mapredInputFormat, Class<K> key, Class<V> value, JobConf job) {
        super(mapredInputFormat, key, value, job);
    }

    public MongoDBInputFormat(InputFormat<K, V> mapredInputFormat, Class<K> key, Class<V> value) {
        super(mapredInputFormat, key, value);
    }
}
