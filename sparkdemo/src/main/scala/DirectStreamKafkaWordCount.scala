import demo.KafkaCluster
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

/**
  * Created by johnliu on 15/10/15.
  */
object DirectStreamKafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    StreamingUtil.setStreamingLogLevels()

    val Array(brokers, topics, zkConnect, kafkaGroupId) = args

    val kafkaParams = Map("zookeeper.connect" -> zkConnect,
      "group.id" -> kafkaGroupId,
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest"
    )

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint4direct")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    messages.foreachRDD(
      rdd => {
        val offsetLists = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val kc = new KafkaCluster(kafkaParams)
        for (offsets <- offsetLists) {
          val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
          val o = kc.setConsumerOffsets(kafkaGroupId, Map((topicAndPartition, offsets.untilOffset)))
          if (o.isLeft) {
            println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
          }
        }
      }
    )


    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 1)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
