package Streaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SparkStreaming12_GraceClose {
  def main(args: Array[String]): Unit = {

    //优雅的关闭
    //1 创建上下文环境对象
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("wc")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
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
    // 启动新的线程，希望在特殊的场合关闭SparkStreaming
    new Thread(new Runnable {
      override def run(): Unit = {

        while ( true ) {
          try {
            Thread.sleep(5000)
          } catch {
            case ex : Exception => println(ex)
          }

          // 监控HDFS文件的变化
          val fs: FileSystem = FileSystem.get(new URI("hdfs://linux1:9000"), new Configuration(), "root")

          val state: StreamingContextState = sc.getState()
          // 如果环境对象处于活动状态，可以进行关闭操作
          if ( state == StreamingContextState.ACTIVE ) {

            // 判断路径是否存在
            val flg: Boolean = fs.exists(new Path("hdfs://hadoop102:9000/stopSpark2"))
            if ( flg ) {
              //关闭采集器和Driver:优雅的关闭
              sc.stop(true, true)
              System.exit(0)
            }

          }
        }

      }
    }).start()
    //启动SparkStreamingContext
    //流式数据处理当中,上下文环境对象是不能停止的
    //main方法不能执行完毕,因为一旦main方法执行完毕.环境对象就会被回收
    //sc.stop()
    //启动采集器
    sc.start()
    //Driver程序等待采集器的执行完毕
    sc.awaitTermination()



    //sc.stop(true,true)

  }
}
