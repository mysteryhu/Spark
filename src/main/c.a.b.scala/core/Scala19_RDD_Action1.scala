package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala19_RDD_Action1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)
    //行动算子 - reduce
    val rdd: RDD[Int] = sc.makeRDD(List(1,4,3,2))

    val i = rdd.reduce(_+_)

    //println(i)
    //行动算子 - collect
    rdd.collect().foreach(println)
    //行动算子 - count
    println(rdd.count())
    //行动算子 - first
    println(rdd.first())
    //行动算子 - take
    println(rdd.take(1))
    //行动算子 - takeOrder
    println(rdd.takeOrdered(2).mkString(","))
    sc.stop()
  }
}
