package com.cbxg.sql.connector.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author:cbxg
 * @date:2021/4/5
 * @description: table api kafka connector
 */
public class TableKafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(
                new Kafka()
                .version("universal")
                .topic("sensor")
                .startFromEarliest()
                .property("group.id","").property("bootstrap.servers","hadoop102:9092")
        )
         .withFormat(new Json())
         .withSchema(new Schema()
                 .field("id", DataTypes.STRING())
                 .field("ts",DataTypes.BIGINT())
                 .field("vc", DataTypes.INT())
         )
         .createTemporaryTable("sensor");

        Table sensor = tableEnv.from("sensor");
        Table select = sensor.where($("id").isEqual("sensor_01"))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sum_vc"))
                .select($("id"), $("sum_vc"));

        DataStream<Tuple2<Boolean, Row>> resultDataStream = tableEnv.toRetractStream(select, Row.class);
        resultDataStream.print();

        env.execute("KafkaConnector");
    }
}
