package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala22_RDD_Cache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)
    //行动算子 - reduce
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

    val rdd1 = rdd.map(a=>(a,1))
    val rdd2 = rdd1.reduceByKey(_+_)

    //println(rdd2.toDebugString)

    //缓存不能切断血缘关系,因为缓存可能丢失,丢失后需要重新根据血缘关系进行查找
    val nocache: RDD[String] = rdd.map(a=>a.toString+System.currentTimeMillis())

    val cache: RDD[String] = rdd.map(_.toString+System.currentTimeMillis()).cache()
    cache.collect();
    println(cache.toDebugString)
    println(cache.collect.mkString(","))
    println(cache.collect.mkString(","))

  }
}
