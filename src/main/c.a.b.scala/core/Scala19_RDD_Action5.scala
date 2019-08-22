package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala19_RDD_Action5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)
    //行动算子 - reduce
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))


    rdd.collect().foreach(println) //内存中打印
    println("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK")
    rdd.foreach(println) //分布式环境下打印

    //算子前面的代码会在 Driver 执行,
    //Driver Coding
    rdd.foreach{
      //Excutor Coding
      println
    }
  }
}

