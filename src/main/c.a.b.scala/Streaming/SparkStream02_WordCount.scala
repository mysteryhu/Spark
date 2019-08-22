package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SparkStream02_WordCount {
  def main(args: Array[String]): Unit = {

    //1 创建上下文环境对象
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("wc")
    //2 初始化SparkStreaming
    val sc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    //采集数据
    val lineDStream: DStream[String] = sc.textFileStream("D:\\Workspace\\Spark\\in\\")
    val wordStreams: DStream[String] = lineDStream.flatMap(_.split(" "))
    //5 类型转化
    val mapStreams: DStream[(String, Int)] = wordStreams.map((_,1))
    //6.按key聚合
    val reduceStream: DStream[(String, Int)] = mapStreams.reduceByKey(_+_)

    reduceStream.print()


    //启动采集器
    sc.start()
    //Driver程序等待采集器的执行完毕
    sc.awaitTermination()

  }
}
