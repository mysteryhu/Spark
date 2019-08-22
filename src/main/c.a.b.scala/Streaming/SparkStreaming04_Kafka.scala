package Streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}



object SparkStreaming04_Kafka {

  def main(args: Array[String]): Unit = {

    // 创建上下文环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming02_WordCount1")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      Map(
        "zookeeper.connect" -> "hadoop102:2181",
        ConsumerConfig.GROUP_ID_CONFIG -> "atguiguKafkaGroup",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
      ),
      Map(
        "kafka190311" -> 3
      ),
      StorageLevel.MEMORY_ONLY
    )

    val valueDStream: DStream[String] = kafkaDStream.map(_._2)

    //val wordDStream: DStream[String] = valueDStream.flatMap(_.split(" "))
    //        val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_,1))
    //        val wordToCountDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_+_)

    valueDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }


}
