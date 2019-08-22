package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStream01_WordCount {
  def main(args: Array[String]): Unit = {

    //1 创建上下文环境对象
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("wc")
    //2 初始化SparkStreaming
    val sc = new StreamingContext(sparkConf,Seconds(3))
    //3.通过监控端口创建DStream.读进来的数据为一行行
   val lineStreams: ReceiverInputDStream[String] = sc.socketTextStream("hadoop102",9999)
    //4. 将每一行数据切分,形成一个个单词
    val wordStreams: DStream[String] = lineStreams.flatMap(_.split(" "))
    //5 类型转化
    val mapStreams: DStream[(String, Int)] = wordStreams.map((_,1))
    //6.按key聚合
    val reduceStream: DStream[(String, Int)] = mapStreams.reduceByKey(_+_)

    //打印
    reduceStream.print()

    //启动SparkStreamingContext
    //流式数据处理当中,上下文环境对象是不能停止的
    //main方法不能执行完毕,因为一旦main方法执行完毕.环境对象就会被回收
    //sc.stop()
    //启动采集器
    sc.start()
    //Driver程序等待采集器的执行完毕
    sc.awaitTermination()

  }
}
