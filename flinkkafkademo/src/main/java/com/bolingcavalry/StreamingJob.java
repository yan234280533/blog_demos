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

package com.bolingcavalry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import com.bolingcavalry.StreamingJob;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String kafkaTopic = "demo";
		String brokers = "10.0.3.14:9092";

		System.out.printf("Reading-2 from kafka topic %s @ %s\n", kafkaTopic, brokers);
		System.out.println();

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", brokers);

		//数据源配置，是一个kafka消息的消费者
		FlinkKafkaConsumer011<String> consumer =
				new FlinkKafkaConsumer011<>("demo", new SimpleStringSchema(), props);

		DataStream<String>  messageStream = env.addSource(consumer);

		// parse the data, group it, window it, and aggregate the counts
		DataStream<StreamingJob.WordWithCount> windowCounts = messageStream
				.flatMap(new FlatMapFunction<String, StreamingJob.WordWithCount>() {
					@Override
					public void flatMap(String value, Collector<StreamingJob.WordWithCount> out) {
						for (String word : value.split("\\s")) {
							out.collect(new StreamingJob.WordWithCount(value, 1L));
						}
					}
				})
				.keyBy("word")
				.timeWindow(Time.seconds(10))
				.reduce(new ReduceFunction<StreamingJob.WordWithCount>() {
					@Override
					public StreamingJob.WordWithCount reduce(StreamingJob.WordWithCount a, StreamingJob.WordWithCount b) {
						return new StreamingJob.WordWithCount(a.word, a.count + b.count);
					}
				});
		windowCounts.print();

		env.execute("Flink-Kafka demo");
	}

	/**
	 * Data type for words with count.
	 */
	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}
}
