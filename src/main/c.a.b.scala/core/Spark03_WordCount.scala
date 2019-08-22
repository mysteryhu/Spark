package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)
    //从存储系统读取数据形成RDD
    //存储系统路径默认为相对路径,相对当前的项目根路径
    //读取文件的方式是基于HAdoop的读取方式
    //读取文件的数据是一行一行的字符串
    val lineRDD: RDD[String] = sc.textFile("input")
    lineRDD.collect().foreach(println)

    //释放资源
    sc.stop()

  }
}
