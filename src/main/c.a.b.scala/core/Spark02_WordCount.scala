package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //从集合中创建RDD
    //准备Spark配置对象
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val context = new SparkContext(conf)

    //从集合中创建RDD
    //并行
    val value: RDD[Int] = context.parallelize(List(2,4,6,8))
    //生成RDD
//    val rdd = context.makeRDD(List(2,4,6,8))
//    val rdd2: RDD[Int] = rdd.map(_*2)
//    rdd2.collect().foreach(println)
    context.makeRDD(Array(List()))


  }
}
