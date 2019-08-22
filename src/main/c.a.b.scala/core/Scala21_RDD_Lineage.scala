package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala21_RDD_Lineage {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)
    //行动算子 - reduce
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))
    val rdd1: RDD[(Int, Int)] = rdd.map((_,1)).reduceByKey(_+_)
    //函数血缘关系
    println(rdd1.toDebugString)
    //函数依赖关系
    rdd1.dependencies.foreach(println)

    sc.stop()
  }
}
