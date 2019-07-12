/*
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

package Project2;
import static Project2.flink.FlinkConfig.KAFKA_ADDR;
import static Project2.flink.FlinkConfig.KAFKA_PORT;
import static Project2.flink.FlinkConfig.INPUT_TOPIC;

import Project2.deserializer.CommentDeserializer;
import Project2.flink.Query1;
import Project2.flink.Query3;
import Project2.metrics.Query1Metrics;
import Project2.metrics.Query2Metrics;
import Project2.metrics.Query3Metrics;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

//Comment types: Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>

public class Main {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// Use event time included in the tuple
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// Emit latency tracking record every 2 seconds
		env.getConfig().setLatencyTrackingInterval(2000L);
		//env.setParallelism(1);
		//env.setMaxParallelism(1);
		Properties configProperties = new Properties();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				KAFKA_ADDR + ":" + KAFKA_PORT); //kafka bootstrap server address
		configProperties.setProperty("group.id", "a");

		// Register a consumer on INPUT_TOPIC to read dataset tuple
		FlinkKafkaConsumer011<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean,
				Long, String, Long, String, String, Long, String>> commentConsumer =
                new FlinkKafkaConsumer011<>(INPUT_TOPIC, new CommentDeserializer(), configProperties);

		// Extract the event time from the tuple using the field createDate
		commentConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>>() {

			@Override
			public long extractAscendingTimestamp(Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String> element) {
				//System.err.println("Extract: " + Instant.ofEpochMilli(element.f5 * 1000).atZone(ZoneId.systemDefault()).toLocalDateTime());
				return element.f5 * 1000;
			}
		});

		commentConsumer.setStartFromLatest();
		DataStream<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean,
						Long, String, Long, String, String, Long, String>> dataStream = env.addSource(commentConsumer);


		Query1.doQuery(dataStream, configProperties);
		Query2.doQuery(dataStream, configProperties);
		Query3.doQuery(dataStream, configProperties);


		//Query1Metrics.doQuery(dataStream, configProperties);
		//Query2Metrics.doQuery(dataStream, configProperties);
		//Query3Metrics.doQuery(dataStream, configProperties);



		// execute program
		env.execute();
	}
}
