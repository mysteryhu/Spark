package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala11_KV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val numRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",4),("d",1)))

    val numRDD2: RDD[(String, Iterable[Int])] = numRDD1.groupByKey()
    val numRDD3: RDD[(String, Int)] = numRDD2.map {
      case (c, datas) => {
        (c, datas.sum)
      }
    }

    //numRDD3.collect().foreach(println)

    sc.stop()
  }
}
