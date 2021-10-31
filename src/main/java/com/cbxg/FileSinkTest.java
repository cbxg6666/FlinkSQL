package com.cbxg;

import com.alibaba.fastjson.JSON;
import com.cbxg.bean.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.avro.AvroBuilder;
import org.apache.flink.formats.avro.AvroWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.flink.core.fs.Path;

import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
/**
 * @author:cbxg
 * @date:2021/6/6
 * @description: 测试 flink 写入 hdfs
 */
public class FileSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(1000 * 60 * 1, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(
                new FsStateBackend("file:///D:/gitProjects/flink_sql_tutorials/chk")
//                new FsStateBackend(URI.create("hdfs://hadoop102:9000/flink"),URI.create(""))
        );
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(21, Time.of(10, TimeUnit.MINUTES)));
//        String outputPath = "file:\\D:\\gitProjects\\flink_sql_tutorials\\src\\main\\resources\\file";

        Properties sourceKafkaProp = new Properties();
        sourceKafkaProp.setProperty("bootstrap.servers", "hadoop102:9092");
        sourceKafkaProp.setProperty("group.id", "sql01_test_01");
        String sourceTopic = "sql_test1";

        org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer<String> kafkaConsumer = new org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer<>(sourceTopic,
                new SimpleStringSchema(),
                sourceKafkaProp);

        DataStream<String> source = env.addSource(kafkaConsumer).name("source").uid("source").rebalance();

        String outputPath = "hdfs://hadoop102:9000/user/hive/warehouse/test1.db/person2";
//        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Person> map = source.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String s) throws Exception {
                return new Person(s,new Random().nextInt(100),new WaterSensor("sensor_01",100L,10));
            }
        });

        StreamingFileSink<Person> sink1 =  StreamingFileSink
                .forBulkFormat(new Path(outputPath)
                        ,new AvroWriterFactory<>((AvroBuilder<Person>) out -> {
                    Schema schema = ReflectData.get().getSchema(Person.class);
                    DatumWriter<Person> datumWriter = new ReflectDatumWriter<>(schema);
                    DataFileWriter<Person> dataFileWriter = new DataFileWriter<>(datumWriter);
                    dataFileWriter.create(schema, out);
                    return dataFileWriter;
                })
                )
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new BucketAssigner<Person, String>() {

                    @Override
                    public String getBucketId(Person element, Context context) {
                        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH").withZone(ZoneId.of("Asia/Shanghai"));

                        return String.format("dt=%s",
                                timeFormatter.format(Instant.ofEpochMilli(context.currentProcessingTime())));
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return SimpleVersionedStringSerializer.INSTANCE;
                    }
                })
                .build();

        map.addSink(sink1);
        env.disableOperatorChaining();
        env.execute("file-streaming");

    }


}
@Data
class Person{
    String name;
    int age;
    String ws;
    public Person(String name, int age, WaterSensor ws){
        this.name = name;
        this.age = age;
        this.ws = JSON.toJSONString(ws);
    }

}