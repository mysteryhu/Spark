package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala19_RDD_Action3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,4,3,2,5,6),2)

    //rdd.saveAsTextFile("output")
    //序列化相关的格式,sparkSQL会用到
    rdd.saveAsObjectFile("output1")
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",1)))
    //特殊格式才能用
    rdd1.saveAsSequenceFile("output2")

    sc.stop()
  }
}
