package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala13_wordCount {
  def main(args: Array[String]): Unit = {
    //使用Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)
    //读取文件内容,一行行的字符串
    val lineRDD: RDD[String] = sc.textFile("input")
    //扁平化 -> (k,v) -> 分组 -> 结构变化
   // val wordCount1: RDD[(String, Int)] = lineRDD.flatMap(_.split(" ")).groupBy(a=>a).map(a=>(a._1,a._2.size))
    val wordCount2: RDD[(String, Int)] = lineRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //val wordCount3: RDD[(String, Int)] = lineRDD.flatMap(_.split(" ")).map((_,1)).foldByKey(0)(_+_)
    val wordCount4: RDD[(String, Int)] = lineRDD.flatMap(_.split(" ")).map((_,1)).aggregateByKey(0)((x,y)=>x+y,(x,y)=>x+y)
    val wordCount5: RDD[(String, Int)]=lineRDD.flatMap(_.split(" ")).map((_, 1)).combineByKey((v: Int) => (v, 0),
      (c: (Int, Int), v) => (c._1 + v, c._2),
      (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c1._2 + c2._2)).map(a=>(a._1,a._2._1))
    val wordCount6: RDD[(String, Int)] = lineRDD.flatMap(_.split(" ")).map((_, 1)).groupByKey().map{ case(c,words)=>(c,words.sum)}
    val wordCount7: RDD[(String, Int)] = lineRDD.flatMap(_.split(" ")).map((_,1)).groupByKey().mapValues(list=>list.sum)
    val wordCount8: RDD[(String, Int)] = lineRDD.flatMap(_.split(" ")).groupBy(a=>a).mapValues(_.size)
    val wordCount9: RDD[(String, Int)] = lineRDD.flatMap(_.split(" ")).groupBy(a=>a).map{ case (a,l)=>(a,l.size)}
    val wordCount10: collection.Map[String, Long] = lineRDD.flatMap(_.split(" ")).countByValue()


    wordCount7.collect().foreach(println)
  }
}
