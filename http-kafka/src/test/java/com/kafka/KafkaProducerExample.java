package com.kafka;

import com.flume.source.JackSonUtilities;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaProducerExample {

    static String ite = "降低为领导色我呢松开后期我脾我都没v十多年个人狗肉我就饿哦日记气财务拉沃尔夫奖我饿案发of哦啊无金额飞机";


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "wx-kafka-03:9092,wx-kafka-04:9092,wx-kafka-05:9092,wx-kafka-06:9092,wx-kafka-07:9092,wx-kafka-08:9092");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 100; i++) {
            System.out.println(i);

            Map<String, String> item = new HashMap<>();
            item.put(RandomStringUtils.random(10, ite), RandomStringUtils.random(20, ite));
            item.put(RandomStringUtils.random(10, ite), RandomStringUtils.random(20, ite));

            String value = JackSonUtilities.toJsonString(item);

            producer.send(new ProducerRecord<>("dev_tag", Integer.toString(i), value));
        }

        producer.close();

    }
}