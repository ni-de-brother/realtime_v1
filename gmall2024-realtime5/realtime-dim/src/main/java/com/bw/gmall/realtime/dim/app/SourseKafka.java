package com.bw.gmall.realtime.dim.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author xqy
 * @CreateTime 2024-12-12
 */

public class SourseKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").print();
        env.execute();
    }
}
