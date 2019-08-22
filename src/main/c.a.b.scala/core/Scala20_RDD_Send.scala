package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Scala20_RDD_Send {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hadoop","spark","flume"))
    val search = new Search("h")
    //Spark在执行作业之前,会先进行闭包检测,目的在于对闭包所使用的的变量是否序列化进行检测
    val rdd2: RDD[String] = search.getMatch2(rdd)

  }
}

class  Search(query:String) {

  //过滤出包含字符串的数据
  def isMatch(s:String):Boolean={
    s.contains(query)
  }
  //过滤出包含字符串的RDD
  def getMatch1(rdd:RDD[String]):RDD[String]={
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatch2(rdd:RDD[String]):RDD[String]={
    //val q:String = query
    rdd.filter(x=>x.contains(query))
  }
}
