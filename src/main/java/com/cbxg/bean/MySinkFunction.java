package com.cbxg.bean;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author:cbxg
 * @date:2021/7/29
 * @description:
 */
public class MySinkFunction<T>  implements Serializable {
    Class clazz;
    public MySinkFunction( Class clazz){
        this.clazz = clazz;
    }

    public StreamingFileSink<T> getSink(String outputPath){
        return   StreamingFileSink
                .forBulkFormat(new Path(outputPath), ParquetAvroWriters.forReflectRecord(clazz))
                .withBucketAssigner(new BucketAssigner<T, String>() {
                    @Override
                    public String getBucketId(T element, Context context) {
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
                .build();
    }

}
