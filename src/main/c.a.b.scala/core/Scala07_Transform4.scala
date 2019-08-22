package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala07_Transform4 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //算子 - flatMap
    //扁平化
  //    val numRDD = sc.makeRDD(List(List(1,2),List(3,4),List(5,6)))
  //    val newNumRDD: RDD[Int] = numRDD.flatMap(a=>a)

    //算子 - glom
    //将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
//    val numRDD: RDD[Int] = sc.makeRDD(List(1,4,3,2),2)
//    val newNumRDD: RDD[Array[Int]] = numRDD.glom()
//    newNumRDD.collect().foreach(datas=>{
//      println("***")
//      //println(datas.max)
//      datas.foreach(println)
//    })

    //转换算子 - groupBy
//    val numRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
//    val newNumRDD: RDD[(Int, Iterable[Int])] = numRDD.groupBy(num => num % 2)
//    newNumRDD.collect().foreach(println)

    //算子 - filter
//    val stringRDD: RDD[String] = sc.makeRDD(List(("xiaoming"),("zhangsna"),("yiersna"),("xiaoxuesheng")))
//    val newStringRDD: RDD[String] = stringRDD.filter(str=>str.contains("xiao"))
//    newStringRDD.collect().foreach(println)

    //算子 - sample
    //val numRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //抽样数据
    //抽取不放回:false,fraction:抽取的概率,seed:随机数种子
    //抽取放回:true,fraction:抽取的次数,seed:随机数种子
    //随机数种子决定了后面的抽取数字
    //Hbase热点数据/数据倾斜
    //val samRDD: RDD[Int] = numRDD.sample(false,0.5)
    //val samRDD: RDD[Int] = numRDD.sample(true,2)
    //samRDD.collect().foreach(println)

    //算子 - distinct([num Tasks])
    //对原RDD去重返回一个新的RDD
//    val list: RDD[Int] = sc.makeRDD(List(2,1,1,1,1,11,1,1,5,5,6,6,6,6))
//    //val numRDD: RDD[Int] = list.distinct()
//    //分区数量等于任务数量
//    val value = list.distinct(3)
//    //numRDD.collect().foreach(println)
//    value.collect().foreach(println)

    //算子 - coalesce 缩减分区数，用于大数据集过滤后，提高小数据集的执将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]行效率。
    //shuffle :将分区的数据打乱重组(该过程需要落盘)
//    val rdd = sc.parallelize(1 to 16,4)
//    val coalease: RDD[Int] = rdd.coalesce(3)
//    coalease.glom().collect().foreach(list=>{
//      println("((((")
//      list.foreach(println)
//    })

    //算子 - sortBy
    //排序





  }
}
