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
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.XORShiftRandom;

import java.util.Random;
import java.util.UUID;

public class Generator {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool pt = ParameterTool.fromArgs(args);
		// the data generation might be CPU intensive --> run with higher parallelism
		DataStream<String> jsonStream = see.addSource(new SimpleJsonDataGenerator(pt)).setParallelism(pt.getInt("sources", 8));

		jsonStream.flatMap(new ThroughputLogger(5000L)).setParallelism(1);
		//jsonStream.print();

		FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>(
				pt.getRequired("region"),
				pt.getRequired("accessKey"),
				pt.getRequired("secretKey"),
				new SimpleStringSchema());

		kinesis.setFailOnError(true);
		kinesis.setDefaultStream("flink-test");
		kinesis.setDefaultPartition("0");

		jsonStream.addSink(kinesis).setParallelism(1);
		// execute program
		see.execute("Kinesis data generator");
	}

	public static class SimpleJsonDataGenerator extends RichParallelSourceFunction<String> implements StoppableFunction {
		private final ParameterTool pt;
		private boolean running = true;
		public SimpleJsonDataGenerator(ParameterTool pt) {
			this.pt = pt;
		}

		@Override
		public void run(SourceContext<String> sourceContext) throws Exception {
			StringBuilder sb = new StringBuilder();
			long seq = 0;
			Random rnd = new XORShiftRandom();
			long sleep = pt.getLong("sleepTime", -1L);
			while(running) {
				sb.setLength(0);
				sb.append("{\"eventSequence\": \"");
				sb.append(seq++);
				sb.append("\", \"eventUUID\": \"");
				sb.append(new UUID(rnd.nextLong(), rnd.nextLong()).toString());
				sb.append("\", \"eventLocation\": \"");
				sb.append(rnd.nextLong());
				sb.append(",");
				sb.append(rnd.nextLong());
				sb.append("\", \"eventPayload\": \"");
				sb.append(RandomStringUtils.random(rnd.nextInt(2000), 0, 0, true, true, null, rnd));
				sb.append("\", \"eventTime\": \"");
				sb.append(System.currentTimeMillis());
				sb.append("\"}");

				sourceContext.collect(sb.toString());
				if(sleep > 0) {
					Thread.sleep(sleep);
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public void stop() {
			running = false;
		}
	}
}
