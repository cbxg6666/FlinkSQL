package com.cbxg.file;

import com.cbxg.bean.*;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.BulkWriter.Factory;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.orc.vector.Vectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

/**
 * @author:cbxg
 * @date:2021/7/29
 * @description:
 */
public class SinkParquet {
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
            env.enableCheckpointing(1000 * 60 * 1, CheckpointingMode.EXACTLY_ONCE);
            env.setStateBackend(
                    new FsStateBackend("file:///D:/gitProjects/flink_sql_tutorials/chk")
            );
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            Properties sourceKafkaProp = new Properties();
            sourceKafkaProp.setProperty("bootstrap.servers", "hadoop102:9092");
            sourceKafkaProp.setProperty("group.id", "sql01_test_02");
            String sourceTopic = "sql_test1";

            org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer<String> kafkaConsumer = new org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer<>(sourceTopic,
                    new SimpleStringSchema(),
                    sourceKafkaProp);

            DataStream<String> source = env.addSource(kafkaConsumer).name("source").uid("source").rebalance();

            String outputPathPre = "hdfs://hadoop102:9000/user/hive/warehouse/test1.db/";

//            SingleOutputStreamOperator<Person> personStream = source.map(new MapFunction<String, Person>() {
//                @Override
//                public Person map(String s) throws Exception {
//                    return new Person(s,new Random().nextInt(100),new WaterSensor("sensor_01",100L,10));
//                }
//            });
//
//            personStream.print();
//            personStream.addSink(new MySinkFunction<Person>(Person.class).getSink(outputPathPre+"person3"));

            final SingleOutputStreamOperator<Student2> studentStream = source.map(new MapFunction<String, Student2>() {
                @Override
                public Student2 map(String value) throws Exception {
                    return new Student2();
                }
            });
            studentStream.print();
//            studentStream.addSink( StreamingFileSink.forBulkFormat("",ParquetAvroWriters.forSpecificRecord(Student2)))


            studentStream.addSink( StreamingFileSink
                    .forBulkFormat(new Path(outputPathPre+"student2"),
                            ParquetAvroWriters.forReflectRecord(Student2.class)

                    )
                    .withBucketAssigner(new BucketAssigner<Student2, String>() {
                        @Override
                        public String getBucketId(Student2 element, Context context) {
//                        HH点mm分钟ss秒
                            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm").withZone(ZoneId.of("Asia/Shanghai"));

                            return String.format("dt=%s",
                                    timeFormatter.format(Instant.ofEpochMilli(context.currentProcessingTime())));
                        }

                        @Override
                        public SimpleVersionedSerializer<String> getSerializer() {
                            return SimpleVersionedStringSerializer.INSTANCE;
                        }
                    })
                    .withRollingPolicy(OnCheckpointRollingPolicy.build())
                    .build());
            env.disableOperatorChaining();
            env.execute("file-streaming");

        }


    }
// StreamingFileSink
//         .forBulkFormat(new Path(outputPath), ParquetAvroWriters.forReflectRecord(Person.class))
//        .withBucketAssigner(new BucketAssigner<Person, String>() {
//@Override
//public String getBucketId(Person element, Context context) {
//        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH").withZone(ZoneId.of("Asia/Shanghai"));
//
//        return String.format("dt=%s",
//        timeFormatter.format(Instant.ofEpochMilli(context.currentProcessingTime())));
//        }
//
//@Override
//public SimpleVersionedSerializer<String> getSerializer() {
//        return SimpleVersionedStringSerializer.INSTANCE;
//        }
//        })
//        .withRollingPolicy(OnCheckpointRollingPolicy.build())
//        .build();