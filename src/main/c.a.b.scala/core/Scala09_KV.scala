package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Scala09_KV {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val numRDD1: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(2,1),(3,1),(4,1),(4,1)))
    //val numRDD2 = sc.makeRDD(List(4,5,6,7,8))
    //k,v类型的算子的源码不在RDD中,通过隐式转换在PairRDDFunctions中查找
    //可以通过制定的分区器决定数据计算的分区,默认的分区是hashPartitioner
    val numRDD2: RDD[(Int, Int)] = numRDD1.partitionBy(new HashPartitioner(2))

    val numRDD3: RDD[(Int, (Int, Int))] = numRDD2.mapPartitionsWithIndex((index, datas) => {
      datas.map(tuple=>(index, tuple))
    })

    numRDD3.collect().foreach(println)


  }
}
