package com.flume.sink;

import com.flume.Constant;
import com.flume.source.JackSonUtilities;
import com.flume.util.KafkaSinkConstants;
import com.flume.util.LogPrivacyUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;

import static com.flume.util.KafkaSinkConstants.*;

/**
 * A Flume Sink that can publish messages to Kafka.
 * This is a general implementation that can be used with any Flume agent and
 * a channel.
 * The message can be any event and the key is a string that we read from the
 * header
 * For use of partitioning, use an interceptor to generate a header with the
 * partition key
 * <p/>
 * Mandatory properties are:
 * brokerList -- can be a partial list, but at least 2 are recommended for HA
 * <p/>
 * <p/>
 * however, any property starting with "kafka." will be passed along to the
 * Kafka producer
 * Read the Kafka producer documentation to see which configurations can be used
 * <p/>
 * Optional properties
 * topic - there's a default, and also - this can be in the event header if
 * you need to support events with
 * different topics
 * batchSize - how many messages to process in one batch. Larger batches
 * improve throughput while adding latency.
 * requiredAcks -- 0 (unsafe), 1 (accepted by at least one broker, default),
 * -1 (accepted by all brokers)
 * useFlumeEventFormat - preserves event headers when serializing onto Kafka
 * <p/>
 * header properties (per event):
 * topic
 * key
 *
 * @author pengming  
 * @date 2016年11月18日 11:18:06
 * @description
 */
public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);

    private final Properties kafkaProps = new Properties();
    private KafkaProducer<String, byte[]> producer;

    private String topic;
    private int batchSize;


    private KafkaSinkCounter counter;
    private List<Future<RecordMetadata>> kafkaFutures;


    private boolean useAvroEventFormat;
    private String partitionHeader = null;
    private Integer staticPartitionId = null;

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        String eventTopic = null;
        String eventKey = null;

        try {
            long processedEvents = 0;

            transaction = channel.getTransaction();
            transaction.begin();

            kafkaFutures.clear();
            long batchStartTime = System.nanoTime();


            long st = System.currentTimeMillis();


            for (; processedEvents < batchSize; processedEvents += 1) {
                event = channel.take();

                if (event == null) {
                    // no events available in channel
                    if (processedEvents == 0) {
                        result = Status.BACKOFF;
                        counter.incrementBatchEmptyCount();
                    } else {
                        counter.incrementBatchUnderflowCount();
                    }
                    break;
                }

                byte[] eventBody = event.getBody();
                Map<String, String> headers = event.getHeaders();

                eventTopic = headers.get(TOPIC_HEADER);
                eventKey = headers.get(KEY_HEADER);

                /** 默认为空,则发送消息到默认的topic, 如果有值则不发送消息到默认的topic 与 headers.get(TOPIC_HEADER); 共用 */
                String sendDefaultTopic = headers.get(Constant.SEND_DEFAULT_TOPIC);

                if (logger.isTraceEnabled()) {
                    if (LogPrivacyUtil.allowLogRawData()) {
                        logger.trace("{Event} " + eventTopic + " : " + eventKey + " : " + new String(eventBody, "UTF-8"));
                    } else {
                        logger.trace("{Event} " + eventTopic + " : " + eventKey);
                    }
                }
                logger.debug("event #{}", processedEvents);

                // create a message and add to buffer
                long startTime = System.currentTimeMillis();

                Integer partitionId = null;
                try {
                    ProducerRecord<String, byte[]> record;
                    if (staticPartitionId != null) {
                        partitionId = staticPartitionId;
                    }
                    //Allow a specified header to override a static ID
                    if (partitionHeader != null) {
                        String headerVal = event.getHeaders().get(partitionHeader);
                        if (headerVal != null) {
                            partitionId = Integer.parseInt(headerVal);
                        }
                    }
                    /** 发送到指定topic */
                    byte[] bytes = serializeEvent(event, useAvroEventFormat);
                    if (StringUtils.isNotEmpty(eventTopic)) {
                        if (partitionId != null) {
                            record = new ProducerRecord<String, byte[]>(eventTopic, partitionId, eventKey, bytes);
                        } else {
                            record = new ProducerRecord<String, byte[]>(eventTopic, eventKey, bytes);
                        }
                        kafkaFutures.add(producer.send(record, new SinkCallback(startTime)));
                    }
                    /** 发送到默认topic */
                    if (StringUtils.isEmpty(sendDefaultTopic)) {
                        if (partitionId != null) {
                            record = new ProducerRecord<String, byte[]>(topic, partitionId, eventKey, bytes);
                        } else {
                            record = new ProducerRecord<String, byte[]>(topic, eventKey, bytes);
                        }
                        kafkaFutures.add(producer.send(record, new SinkCallback(startTime)));
                    }
                } catch (NumberFormatException ex) {
                    logger.error("Non integer partition id specified", ex);
                } catch (Exception ex) {
                    // N.B. The producer.send() method throws all sorts of RuntimeExceptions
                    // Catching Exception here to wrap them neatly in an EventDeliveryException
                    // which is what our consumers will expect
                    logger.error("Could not send event", ex);
                }
            }

            long end = System.currentTimeMillis();
            //Prevent linger.ms from holding the batch
//            producer.flush();


            long flush = System.currentTimeMillis();

            logger.info("sum #{}, time #{} ms, flush #{} ms", new Object[]{processedEvents, (flush - end), (end - st)});

            // publish batch and commit.
            if (processedEvents > 0) {
                for (Future<RecordMetadata> future : kafkaFutures) {
                    future.get();
                }
                long endTime = System.nanoTime();
                counter.addToKafkaEventSendTimer((endTime - batchStartTime) / (1000 * 1000));
                counter.addToEventDrainSuccessCount(Long.valueOf(kafkaFutures.size()));
            }

            transaction.commit();

        } catch (Exception ex) {
            logger.error("Failed to publish events", ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try {
                    kafkaFutures.clear();
                    transaction.rollback();
                    counter.incrementRollbackCount();
                } catch (Exception e) {
                    logger.error("Transaction rollback failed", e);
                }
            }
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        return result;
    }

    @Override
    public synchronized void start() {
        // instantiate the producer
        producer = new KafkaProducer<String,byte[]>(kafkaProps);
        counter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        counter.stop();
//        logger.info("Kafka Sink {} stopped", getName());
        logger.info("Kafka Sink {} stopped. Metrics: {}", getName(), counter);
        super.stop();
    }


    /**
     * We configure the sink and generate properties for the Kafka Producer
     *
     * Kafka producer properties is generated as follows:
     * 1. We generate a properties object with some static defaults that
     * can be overridden by Sink configuration
     * 2. We add the configuration users added for Kafka (parameters starting
     * with .kafka. and must be valid Kafka Producer properties
     * 3. We add the sink's documented parameters which can override other
     * properties
     *
     * @param context
     */
    @Override
    public void configure(Context context) {

        translateOldProps(context);

        String topicStr = context.getString(TOPIC_CONFIG);
        if (topicStr == null || topicStr.isEmpty()) {
            topicStr = DEFAULT_TOPIC;
            logger.warn("Topic was not specified. Using {} as the topic.", topicStr);
        } else {
            logger.info("Using the static topic {}. This may be overridden by event headers", topicStr);
        }

        topic = topicStr;

        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);

        if (logger.isDebugEnabled()) {
            logger.debug("Using batch size: {}", batchSize);
        }

        useAvroEventFormat = context.getBoolean(KafkaSinkConstants.AVRO_EVENT, KafkaSinkConstants.DEFAULT_AVRO_EVENT);

        partitionHeader = context.getString(KafkaSinkConstants.PARTITION_HEADER_NAME);
        staticPartitionId = context.getInteger(KafkaSinkConstants.STATIC_PARTITION_CONF);

        if (logger.isDebugEnabled()) {
            logger.debug(KafkaSinkConstants.AVRO_EVENT + " set to: {}", useAvroEventFormat);
        }

        kafkaFutures = new LinkedList<Future<RecordMetadata>>();

        String bootStrapServers = context.getString(BOOTSTRAP_SERVERS_CONFIG);
        if (bootStrapServers == null || bootStrapServers.isEmpty()) {
            throw new ConfigurationException("Bootstrap Servers must be specified");
        }

        setProducerProps(context, bootStrapServers);

        if (logger.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
            logger.debug("Kafka producer properties: {}", kafkaProps);
        }

        if (counter == null) {
            counter = new KafkaSinkCounter(getName());
        }
    }

    private void translateOldProps(Context ctx) {

        if (!(ctx.containsKey(TOPIC_CONFIG))) {
            ctx.put(TOPIC_CONFIG, ctx.getString("topic"));
            logger.warn("{} is deprecated. Please use the parameter {}", "topic", TOPIC_CONFIG);
        }

        //Broker List
        // If there is no value we need to check and set the old param and log a warning message
        if (!(ctx.containsKey(BOOTSTRAP_SERVERS_CONFIG))) {
            String brokerList = ctx.getString(BROKER_LIST_FLUME_KEY);
            if (brokerList == null || brokerList.isEmpty()) {
                throw new ConfigurationException("Bootstrap Servers must be specified");
            } else {
                ctx.put(BOOTSTRAP_SERVERS_CONFIG, brokerList);
                logger.warn("{} is deprecated. Please use the parameter {}", BROKER_LIST_FLUME_KEY, BOOTSTRAP_SERVERS_CONFIG);
            }
        }

        //batch Size
        if (!(ctx.containsKey(BATCH_SIZE))) {
            String oldBatchSize = ctx.getString(OLD_BATCH_SIZE);
            if ( oldBatchSize != null  && !oldBatchSize.isEmpty())  {
                ctx.put(BATCH_SIZE, oldBatchSize);
                logger.warn("{} is deprecated. Please use the parameter {}", OLD_BATCH_SIZE, BATCH_SIZE);
            }
        }

        // Acks
        if (!(ctx.containsKey(KAFKA_PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG))) {
            String requiredKey = ctx.getString(KafkaSinkConstants.REQUIRED_ACKS_FLUME_KEY);
            if (!(requiredKey == null) && !(requiredKey.isEmpty())) {
                ctx.put(KAFKA_PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG, requiredKey);
                logger.warn("{} is deprecated. Please use the parameter {}", REQUIRED_ACKS_FLUME_KEY,
                        KAFKA_PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG);
            }
        }

        if (ctx.containsKey(KEY_SERIALIZER_KEY )) {
            logger.warn("{} is deprecated. Flume now uses the latest Kafka producer which implements " +
                            "a different interface for serializers. Please use the parameter {}",
                    KEY_SERIALIZER_KEY,KAFKA_PRODUCER_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        }

        if (ctx.containsKey(MESSAGE_SERIALIZER_KEY)) {
            logger.warn("{} is deprecated. Flume now uses the latest Kafka producer which implements " +
                            "a different interface for serializers. Please use the parameter {}",
                    MESSAGE_SERIALIZER_KEY,
                    KAFKA_PRODUCER_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        }
    }

    private void setProducerProps(Context context, String bootStrapServers) {
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS);
        //Defaults overridden based on config
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIAIZER);
        kafkaProps.putAll(context.getSubProperties(KAFKA_PRODUCER_PREFIX));
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    }

    /**
     * Serialize event byte [ ].
     *
     * @param event              the event
     * @param useAvroEventFormat the use avro event format
     * @return the byte [ ]
     * @throws Exception the exception
     * @author pengming
     * @date 2016年11月18日 11:18:06
     * @description
     * @responseBody
     * @requestPayLoad
     */
    public byte[] serializeEvent(Event event, boolean useAvroEventFormat) throws Exception {
        byte[] bytes;
        byte[] body = event.getBody();
        if (useAvroEventFormat) {
            Map<String, Object> eventItem = new HashMap<>();
            eventItem.put("headers", event.getHeaders());
            eventItem.put("body", body != null && body.length > 0 ? JackSonUtilities.readJsonNode(body) : body);
            bytes = JackSonUtilities.toBytes(eventItem);
        } else {
            bytes = body;
        }
        logger.info("push: " + JackSonUtilities.toJsonString(event.getHeaders()));
        return bytes;
    }

    private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
        Map<CharSequence, CharSequence> charSeqMap = new HashMap<CharSequence, CharSequence>();
        for (Map.Entry<String, String> entry : stringMap.entrySet()) {
            charSeqMap.put(entry.getKey(), entry.getValue());
        }
        return charSeqMap;
    }


    /**
     * Gets topic.
     *
     * @return the topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Gets batch size.
     *
     * @return the batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Gets kafka props.
     *
     * @return the kafka props
     */
    protected Properties getKafkaProps() {
        return kafkaProps;
    }

}

/**
 * The type Sink callback.
 *
 * @author pengming  
 * @date 2016年11月18日 11:18:06
 * @description
 */
class SinkCallback implements Callback {
    private static final Logger logger = LoggerFactory.getLogger(SinkCallback.class);
    private long startTime;

    /**
     * Instantiates a new Sink callback.
     *
     * @param startTime the start time
     */
    public SinkCallback(long startTime) {
        this.startTime = startTime;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            logger.debug("Error sending message to Kafka {} ", exception.getMessage());
        }

        if (logger.isDebugEnabled()) {
            long eventElapsedTime = System.currentTimeMillis() - startTime;
            logger.debug("Acked message partition:{} ofset:{}",  metadata.partition(), metadata.offset());
            logger.debug("Elapsed time for send: {}", eventElapsedTime);
        }
    }
}

