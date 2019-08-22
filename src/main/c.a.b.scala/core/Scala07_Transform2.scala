package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala07_Transform2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)


    //转换算子 -mapPartitions  可迭代的集合-datas
    val newNumRdd: RDD[Int] = numRDD.mapPartitions(datas => {
      println("((((((") //运行了两次  两个分区
      datas.map(_ * 2)
    })

    newNumRdd.collect().foreach(println)
  }
}
