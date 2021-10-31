package com.cbxg.kafka;

/**
 * @author:cbxg
 * @date:2021/5/3
 * @description: kafka 2 阶段提交的消费者
 */
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
public class ConsumerKafkaReadCommitted {
    public static void main(String[] args) {
        //创建消费者
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"group01");

        //设置事务的隔离级别   如果事务没有提交  读取不到
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("two_page_sink"));
        try {
            while (true){
                //设置间隔多长时间取一次数据
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                //判断数据是否是空的
                if(!consumerRecords.isEmpty()){
                    Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
                    while (iterator.hasNext()){
                        ConsumerRecord<String, String> next = iterator.next();
                        String topic = next.topic();
                        String key = next.key();
                        String value = next.value();
                        long offset = next.offset();
                        int partition = next.partition();
                        long timestamp = next.timestamp();


                        System.out.println(new Date() + "\t"+ "key = " + key+"\t"+"offset = " + offset+"\t"+"value = " + value+"\t"+"partition = " + partition+"\t"+"timestamp = " + timestamp+"\t"+"topic = " + topic);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            kafkaConsumer.close();
        }

    }
}

