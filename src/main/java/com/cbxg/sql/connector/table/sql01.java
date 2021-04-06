package com.cbxg.sql.connector.table;

import com.cbxg.sql.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author:cbxg
 * @date:2021/4/5
 * @description:
 */
public class sql01 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> streamSource = env.fromElements(
                new WaterSensor("sensor_01", 100000L, 100),
                new WaterSensor("sensor_02", 100000L, 100),
                new WaterSensor("sensor_01", 100000L, 120),
                new WaterSensor("sensor_03", 100000L, 110)
        );


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(streamSource);
//        table.printSchema();
//        root
//                |-- f0: LEGACY('RAW', 'ANY<com.cbxg.sql.bean.WaterSensor>')
        Table select = table
                .where($("id").isEqual("sensor_01"))
//                .where($("cv").isGreaterOrEqual(110))
                .groupBy("id")
                .aggregate($("vc").sum().as("vc_sum"))
                .select($("id"),$("vc_sum"));


        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(select, Row.class);

        tuple2DataStream.print();

        env.execute("flink_01");
    }
}
