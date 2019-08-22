package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala19_RDD_Action2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,4,3,2,5,6),2)

    //行动算子 -aggregate
    //算子中的0值,在分区内可以使用,同时分区间计算也会用到
    val i = rdd.aggregate(0)(_+_,_+_)

    //行动算子 - fold
    val j: Int = rdd.fold(0)(_+_)
    println(j)

    sc.stop()
  }
}
