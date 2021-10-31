package com.cbxg.cdc;

/**
 * @author:cbxg
 * @date:2021/5/8
 * @description:
 */
import com.sun.jersey.api.client.WebResource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;

public class MySqlBinlogSourceExample {
    public static void main(String[] args) throws Exception {

        //initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
        //latest-offset: Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
        //timestamp: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified timestamp. The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
        //specific-offset: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
        Properties properties = new Properties();
        properties.setProperty("scan.startup.mode", "initial");


        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall") // monitor all tables under inventory database
                .tableList("gmall.student")
                .username("root")
                .password("000000")
                .debeziumProperties(properties)
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<String> source = env
                .addSource(sourceFunction);
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("=================="+value);
                return value;
            }
        });

        env.execute();
    }
}