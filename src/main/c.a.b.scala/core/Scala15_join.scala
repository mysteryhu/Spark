package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala15_join {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)
   //转换算子 -join
    //(Int,String)
    val rdd1 = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c")))
    val rdd2 = sc.makeRDD(List((1,"aa"),(2,"bb"),(3,"cc"),(4,"dd")))
    val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)

    //转换算子 - cogroup
    val rdd4: RDD[(Int, (Iterable[String], Iterable[String]))] = rdd1.cogroup(rdd2)
    rdd4.collect().foreach(println)
    rdd3.collect().foreach(println)
  }
}
