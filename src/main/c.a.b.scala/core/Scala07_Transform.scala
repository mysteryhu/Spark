package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala07_Transform {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //转换算子 -map
    //分布式计算概念
    //Driver Coding
        //所有的算子的逻辑的计算操作都在executor中执行
    val newNumRDD: RDD[Int] = numRDD.map(
      num => { //Executor
        num * 2
      })
    newNumRDD.collect().foreach(println)
  }
}
