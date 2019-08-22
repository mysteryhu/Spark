package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala19_RDD_Action4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)),2)
    rdd.flatMap(a=>a._1*a._2)

    //行动算子 - CountByKey
    //val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    val tupleToLong: collection.Map[(String, Int), Long] = rdd.countByValue()
    //println(tupleToLong)
  }
}
