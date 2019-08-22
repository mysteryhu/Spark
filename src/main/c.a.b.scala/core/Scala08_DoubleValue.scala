package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala08_DoubleValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val numRDD1 = sc.makeRDD(List(1,2,3,4,4))
    val numRDD2 = sc.makeRDD(List(4,5,6,7,8))
    // 算子 union 交集
    val numRDD3: RDD[Int] = numRDD1.union(numRDD2)

    // 算子 subtract 差集
    val numRDD4 = numRDD1.subtract(numRDD2)

    //算子 interception 交集

    val numRDD5: RDD[Int] = numRDD1.intersection(numRDD2)

    //算子 - cartesian 笛卡尔乘积
    val numRDD6: RDD[(Int, Int)] = numRDD1.cartesian(numRDD2)

    //算子 zip 拉链
    val numRDD7: RDD[(Int, Int)] = numRDD1.zip(numRDD2)
    numRDD7.collect().foreach(println)
  }
}
