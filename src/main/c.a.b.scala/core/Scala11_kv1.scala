package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala11_kv1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

   val rdd = sc.makeRDD(List(("a",1),("b",2),("b",3),("a",3),("b",4),("a",5)),2)
    //取出每个分区相同key对应值的最大值,然后相加
    //("a",1),("b",2),("b",3) = > ("a",1)("b",3)
    //("a",3),("b",4),("a",5) = > ("a",5)("b",4)
       //("a",6) ("b",7)

    //aggregateByKey使用了函数柯里化
    //存在两个参数列表
    //第二个参数需要传递两个参数
    //第一个参数表示分区内计算规则
    //第二个参数表示分区间计算规则
    //分区内规则和分区间规则一致=>aggregateByKey的简化版 --foldByKey
    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(10)((x, y) => {
      Math.max(x, y)
    }, (x, y) => {
      x + y
    })
    //rdd.aggregateByKey(0)((x,y)=>{Math.max(x,y)},(_+_))
    //rdd.foldByKey()
    rdd1.collect().foreach(println)
  }
}
