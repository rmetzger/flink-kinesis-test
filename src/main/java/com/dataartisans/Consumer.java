package com.dataartisans;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.XORShiftRandom;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class Consumer {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool pt = ParameterTool.fromArgs(args);

		Properties props = new Properties();
		props.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, pt.get("accesskey"));
		props.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, pt.get("secretkey"));
		props.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "eu-central-1");
		props.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, pt.get("start", "TRIM_HORIZON"));

		FlinkKinesisConsumer<String> consumer = new FlinkKinesisConsumer<>(pt.getRequired("stream"), new SimpleStringSchema(), props);
		DataStream<String> jsonStream = see.addSource(consumer);

		jsonStream.flatMap(new ThroughputLogger(5_000L)).setParallelism(1);
		jsonStream.flatMap(new LatencyStat());
		//jsonStream.print();

		// execute program
		see.execute("Kinesis data consumer");
	}

	private static class LatencyStat implements FlatMapFunction<String, Object> {
		private static final Logger LOG = LoggerFactory.getLogger(LatencyStat.class);

		ObjectMapper om = null;
		DescriptiveStatistics stats = new DescriptiveStatistics(60_000);
		long cnt = 0;
		@Override
		public void flatMap(String s, Collector<Object> collector) throws Exception {
			if(om == null) {
				om = new ObjectMapper();
			}
			ObjectNode on = om.readValue(s, ObjectNode.class);
			long time = on.get("eventTime").getValueAsLong();
			long lat = System.currentTimeMillis() - time;
			stats.addValue(lat);
			if(cnt++ % 6000 == 0) {
				LOG.info("Last latency {} " +
						"min {} mean {} max {} stddev {} " +
						"p25 {} p50 {} p75 {} p90 {} p99 {}", lat, stats.getMin(), stats.getMean(), stats.getMax(), stats.getStandardDeviation(),
						stats.getPercentile(0.25), stats.getPercentile(0.5), stats.getPercentile(0.75), stats.getPercentile(0.9), stats.getPercentile(0.99));
			}

		}
	}
}
