package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala12_combineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)
    //计算每种key的均值
    val rdd: RDD[(String, Int)] = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
    //分区内第一次碰见key的时候,将数据v进行结构的转变
    //v -> (v,1)
    //第一个参数将第一个key出现的v结构转换规则
    //第二个参数表示分区内计算规则  ("a",88) ->
    //第三个参数表示分区间计算规则
    val rdd1: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (v: Int) => (v, 1),      //("a",88) -> (88,1)
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),  // (88,1) (95,1) -> (88+95,1+1)
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) //每个分区相同key的 v做运算
    )
    val rdd2: RDD[(String, Int)] = rdd1.map((x)=> (x._1,x._2._1/x._2._2))
    rdd2.collect().foreach(println)



  }

}
