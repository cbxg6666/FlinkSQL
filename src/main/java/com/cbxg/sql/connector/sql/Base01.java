package com.cbxg.sql.connector.sql;

import com.cbxg.sql.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author:cbxg
 * @date:2021/4/6
 * @description: 简单的一个注册表查询的实例
 */
public class Base01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<WaterSensor> streamSource = env.fromElements(
                new WaterSensor("sensor_01", 100000L, 100),
                new WaterSensor("sensor_02", 100000L, 100),
                new WaterSensor("sensor_01", 100000L, 120),
                new WaterSensor("sensor_03", 100000L, 110)
        );
        Table table = tableEnv.fromDataStream(streamSource);

        tableEnv.createTemporaryView("sensor",table);

        Table query = tableEnv.sqlQuery("select * from sensor where id ='sensor_01'");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(query, Row.class);
        rowDataStream.print();


        env.execute("Base01");
    }
}
