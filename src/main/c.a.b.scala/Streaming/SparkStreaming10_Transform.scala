package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SparkStreaming10_Transform {
  def main(args: Array[String]): Unit = {
    //1 创建上下文环境对象
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("wc")
    //2 初始化SparkStreaming
    val sc = new StreamingContext(sparkConf,Seconds(3))
    sc.checkpoint("/checkpoint")
    //3.通过监控端口创建DStream.读进来的数据为一行行
    val lineStreams: ReceiverInputDStream[String] = sc.socketTextStream("hadoop102",9999)
    //4. 将每一行数据切分,形成一个个单词
    val wordStreams: DStream[String] = lineStreams.flatMap(_.split(" "))
    //5 类型转化(word,1L)
    val mapStreams: DStream[(String, Long)] = wordStreams.map((_,1L))
    //6.按key聚合
    //val reduceStream: DStream[(String, Int)] = mapStreams.reduceByKey(_+_)
    //    //使用有状态的操作完成数据的聚合
    //DStream有状态操作是依赖于检查点完成的.
    val stateDstream: DStream[(String, Long)] = mapStreams.updateStateByKey[Long](
      (seq: Seq[Long], buffer: Option[Long]) => {
        val s: Long = seq.sum + buffer.getOrElse(0L)
        Option(s)
      }
    )
    //DriverCoding(1)
    stateDstream.map{
      case (k,v)=>{
        //ExecutorCoding(N)
        v
      }
    }

    //DriverCoding(1)
    stateDstream.transform(
      rdd=>{
        //DriverCoding(执行算子的个数 数 M = 一个采集周期是一个算子)
        rdd.map{
          case (k,v )=> {
            //ExecutorCoding(N)
            v
          }
        }
      }
    )
    //打印结果
    stateDstream.print()

    //打印
    //.print()

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
