package Streaming

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.util.Random

object SparkStreaming07_MockData {
    //生产数据
    def main(args: Array[String]): Unit = {

      val properties = new Properties()
      properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
      properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](properties)

      // 模拟数据
      while ( true ) {
        for ( i <- 0 until new Random().nextInt(50) ) {
          val time: Long = System.currentTimeMillis()
          // 向kafka中生产数据
          producer.send(new ProducerRecord[String, String]("kafka190311", time.toString))
          println(time)
        }

        Thread.sleep(2000)

      }
    }

}
