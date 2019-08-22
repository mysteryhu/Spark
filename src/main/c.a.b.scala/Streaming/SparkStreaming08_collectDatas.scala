package Streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreaming08_collectDatas {
  def main(args: Array[String]): Unit = {

    // 创建上下文环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming08_collectDatas")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

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

    val windowDStream: DStream[String] = valueDStream.window(Seconds(60),Seconds(10))
    //一分钟内 每十秒的广告点击趋势图

    val mapDS: DStream[(String, Long)] = windowDStream.map {
      time => {
        val t = time.substring(0, time.length - 4) + "0000"
        (t, 1L)
      }
    }

    val reDS: DStream[(String, Long)] = mapDS.reduceByKey(_+_)

    reDS.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
