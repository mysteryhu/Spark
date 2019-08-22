package core

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala10_definePartitioner {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val numRDD1: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(2,1),(3,1),(4,1),(4,1)))

    //自定义分区器,将所有的数据放在一号分区
      //使用自定义分区器
      val numRDD2: RDD[(Int, Int)] = numRDD1.partitionBy(new MyPartitioner(2))

      val numRDD3: RDD[(Int, (Int, Int))] = numRDD2.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    numRDD3.collect().foreach(println)

  }
}

class MyPartitioner (num:Int) extends Partitioner{

  def numPartitions: Int = num
  def getPartition(key: Any): Int = 1
}
