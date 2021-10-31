package com.cbxg.state;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author:cbxg
 * @date:2021/8/3
 * @description:
 */
public class ChkRestoreStateTest {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(1000 * 60 * 1, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(
                new FsStateBackend("file:///D:/gitProjects/flink_sql_tutorials/chk")
        );
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties sourceKafkaProp = new Properties();
        sourceKafkaProp.setProperty("bootstrap.servers", "hadoop102:9092");
        sourceKafkaProp.setProperty("group.id", "stored_state");
        String sourceTopic = "sql_test1";

        final FlinkKafkaConsumer<String> kafkaConsumer =
                new FlinkKafkaConsumer<String>(sourceTopic, new SimpleStringSchema(),sourceKafkaProp);
        final DataStreamSource<String> source = env.addSource(kafkaConsumer);

    }
}
