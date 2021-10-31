package com.cbxg.file;

import com.cbxg.bean.*;

import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.vector.RowDataVectorizer;
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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.apache.orc.TypeDescription;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

/**
 * @author:cbxg
 * @date:2021/7/29
 * @description:
 */
public class SinkORC {
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


          final SingleOutputStreamOperator<Student2> studentStream = source.map(new MapFunction<String, Student2>() {
              @Override
              public Student2 map(String value) throws Exception {
                  return new Student2();
              }
          });

          //定义类型和字段名
          LogicalType[] orcTypes = new LogicalType[]{
                  new VarCharType(), new IntType(), new BooleanType(),new DoubleType()};

          Field[] declaredFields = Student2.class.getDeclaredFields();
          ArrayList<String> fields = new ArrayList<>();
          for (Field declaredField :declaredFields ) {
              fields.add(declaredField.getName());
          }

          String[] strings = fields.toArray(new String[0]);

          TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(RowType.of(
                  orcTypes,
                  strings));

          //构造工厂类OrcBulkWriterFactory
          final OrcBulkWriterFactory<RowData> factory = new OrcBulkWriterFactory<>(
                  new RowDataVectorizer(typeDescription.toString(), orcTypes));

          final SingleOutputStreamOperator<RowData> map = studentStream.map(new MapFunction<Student2, RowData>() {
              @Override
              public RowData map(Student2 student2) throws Exception {
                  GenericRowData rowData = new GenericRowData(4);
                  rowData.setField(0, StringData.fromString(student2.getName()));
                  rowData.setField(1, student2.getAge());
                  rowData.setField(2, student2.is_cool());
                  rowData.setField(3, student2.getDb());
                  return rowData;
              }
          });
          map.print();
          map.addSink( StreamingFileSink
                  .forBulkFormat(new Path(outputPathPre+"student2"),
                          factory
                  )
                  .withBucketAssigner(new BucketAssigner<RowData, String>() {

                      @Override
                      public String getBucketId(RowData element, Context context) {
                          DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm").withZone(ZoneId.of("Asia/Shanghai"));
                          final long reportTime = element.getLong(2);
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