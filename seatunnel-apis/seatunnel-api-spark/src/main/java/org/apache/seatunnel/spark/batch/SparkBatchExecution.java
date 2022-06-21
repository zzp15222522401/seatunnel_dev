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

package org.apache.seatunnel.spark.batch;

import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.spark.BaseSparkTransform;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.spark.utils.DataCountRecord;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class SparkBatchExecution implements Execution<SparkBatchSource, BaseSparkTransform, SparkBatchSink, SparkEnvironment> {

    private final SparkEnvironment environment;

    private Config config = ConfigFactory.empty();

    public SparkBatchExecution(SparkEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public void start(List<SparkBatchSource> sources, List<BaseSparkTransform> transforms, List<SparkBatchSink> sinks) throws SQLException, IOException, ClassNotFoundException {

        sources.forEach(source -> SparkEnvironment.registerInputTempView(source, environment));

        if (!sources.isEmpty()) {
            Dataset<Row> ds = sources.get(0).getData(environment);
            for (BaseSparkTransform transform : transforms) {
                ds = SparkEnvironment.transformProcess(environment, transform, ds);
                SparkEnvironment.registerTransformTempView(transform, ds);
            }
            for (SparkBatchSink sink : sinks) {
                SparkEnvironment.sinkProcess(environment, sink, ds);
            }
            DataCountRecord.dataCount(environment, sources, transforms, ds);
        }
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

}

