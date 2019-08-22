package Streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver

import scala.util.control.Breaks

object SparkStream03_SourceFile {

  def main(args: Array[String]): Unit = {

    //1 创建上下文环境对象
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("wc")
    //2 初始化SparkStreaming
    val sc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    //自定义数据源采集器
    val lineDSteam: ReceiverInputDStream[String] = sc.receiverStream(new MyReceiver("hadoop102",9999))
    //数据处理
    val reduceStream: DStream[(String, Int)] = lineDSteam.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //打印结果
    reduceStream.print()

    //启动采集器
    sc.start()
    //Driver程序等待采集器的执行完毕
    sc.awaitTermination()
  }
}

//自定义采集器
//1.继承Receiver 抽象类
//2.重写onStart() ,onStop()
class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY ) with Serializable{
  private var socket:Socket = _

  def receive(): Unit ={
    socket = new Socket(host,port)
    val reader = new BufferedReader(
      new InputStreamReader(
        socket.getInputStream, "UTF-8"
      )
    )

    //FileIO
    var s =""
    Breaks.breakable{
      while ((s=reader.readLine())!=null){
          if("==END==".equals(s)){
            Breaks.break()
          }else{
            store(s) // 存储一行数据
          }
      }
    }
  }


  override def onStart(): Unit = {
    new Thread(new Runnable {
        override def run(): Unit = {
          receive()
        }
      }).start()
  }

  override def onStop(): Unit = {
    if(socket != null){
      if(!socket.isClosed){
        socket.close()
        socket = null
      }
    }
  }
}
