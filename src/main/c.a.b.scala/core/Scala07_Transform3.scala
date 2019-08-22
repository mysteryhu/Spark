package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala07_Transform3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)


    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    //转换算子 - mapPartitionsWithIndex
    val newNumRDD: RDD[String] = numRDD.mapPartitionsWithIndex((index, datas) => {
      datas.map(x => "[partID:" + index + ",value:" + x + "]")
    })

    newNumRDD.collect().foreach(println)
  }
}
