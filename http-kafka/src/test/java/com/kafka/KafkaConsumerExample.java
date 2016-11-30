package com.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerExample {

    private static final String TOPIC = "dev_tag";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("zookeeper.connect", "zk9:2181,zk0:2181,zk1:2181,zk3:2181,zk4:2181");
        props.put("auto.offset.reset","smallest");
        props.put("group.id", "group2");
        props.put("enable.auto.commit", "true");
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig consumerConfig =  new kafka.consumer.ConsumerConfig(props);
        ConsumerConnector consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<>();
        int localConsumerCount = 1;
        topicCountMap.put(TOPIC, localConsumerCount);
//        topicCountMap.put("test", localConsumerCount);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TOPIC);
        streams.stream().forEach(stream -> {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> next = it.next();
                System.out.printf("topic = %s, offset = %d, key = %s, value = %s\n", next.topic(), next.offset(), next.key(), new String(next.message()));
            }
        });

    }
}