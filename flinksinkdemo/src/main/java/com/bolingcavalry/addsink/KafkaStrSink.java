package com.bolingcavalry.addsink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author will
 * @email zq2599@gmail.com
 * @date 2020-03-14 22:08
 * @description kafka发送字符串的sink
 */
public class KafkaStrSink {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //并行度为1
        env.setParallelism(1);

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the Kafka reader
        String kafkaTopic = params.get("topic","write");
        String brokers = params.get("brokers", "10.0.3.14:9092");

        System.out.printf("Reading-4 from kafka topic %s @ %s\n", kafkaTopic, brokers);
        System.out.println();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(kafkaTopic,
                new ProducerStringSerializationSchema(kafkaTopic),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);


        //创建一个List，里面有两个Tuple2元素
        List<String> list = new ArrayList<>();
        list.add("aaa");
        list.add("bbb");
        list.add("ccc");
        list.add("ddd");
        list.add("eee");
        list.add("fff");
        list.add("aaa");

        //统计每个单词的数量
        env.fromCollection(list)
           .addSink(producer)
           .setParallelism(4);

        env.execute("sink demo : kafka str");
    }
}
