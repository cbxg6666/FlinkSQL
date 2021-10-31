//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStreamSink;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.CheckpointConfig;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
//import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
//import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//
//import java.util.Arrays;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//
//public class Consumer1 {
//    public static void main(String[] args) throws Exception {
//
//        Properties prop = new Properties();
//        prop.put("bootstrap.servers","localhost:9092");
//        prop.put("group.id","test");
//        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        prop.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(1000 * 60 * 1, CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new FsStateBackend("file:///Users/apple/IdeaProjects/TestProject/fkinktest/checkpoint"));
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(20, Time.of(3, TimeUnit.MINUTES)));
//
//        env.setParallelism(2);
//        DataStreamSource<Tuple2<String, String>> stream = env.addSource(new FlinkKafkaConsumer<>("mytopic", new CustomKafkaDeserializationSchema(), prop));
//
//
//        SingleOutputStreamOperator<String> map = stream.map(new MapFunction<Tuple2<String, String>, String>() {
//            @Override
//            public String map(Tuple2<String, String> value) throws Exception {
//                return  "partition:" + value.f0 + "vaule:" + value.f1;
//            }
//        });
//        map.print();
//
//        Properties sinkProp = new Properties();
//        sinkProp.put("bootstrap.servers","localhost:9092");
//        sinkProp.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        sinkProp.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        sinkProp.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"10000");
//        sinkProp.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"my-transaction");
//        sinkProp.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//
//        sinkProp.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
//
//        sinkProp.setProperty("transaction.timeout.ms",1000*60*5+"");
//        FlinkKafkaProducer<String> sinktopic = new FlinkKafkaProducer("sinktopic",
//                new MyKafkaSerializationSchema(),
//                sinkProp,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
//
//        // fault-tolerance
////        FlinkKafkaProducer<String> sinktopic = new FlinkKafkaProducer(
////                "my-topic",                  // target topic
////                new SimpleStringSchema(),    // serialization schema
////                sinkProp,                  // producer config
////                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
//
////        FlinkKafkaProducer011 flinkKafkaProducer011 = new FlinkKafkaProducer011();
//
//        map.addSink(sinktopic);
//
//
//        env.execute("test1");
//
//    }
//}
