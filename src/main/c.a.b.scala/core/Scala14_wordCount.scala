package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala14_wordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)
    //一次读一行
    val lineRDD: RDD[String] = sc.textFile("input")
    //①
    //扁平化
    val wordRDD: RDD[String] = lineRDD.flatMap(a=>a.split(" "))
    //按key分组
    val strRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(a=>a)
    //(a,(1,1,1,1,1,)) -> (a,5)
    val wordCount1: RDD[(String, Int)] = strRDD.map(a=>(a._1,a._2.size))

    //②
    //扁平化
    val wordRDD2: RDD[String] = lineRDD.flatMap(a=>a.split(" "))
    //按key分组
    val strRDD2: RDD[(String, Iterable[String])] = wordRDD2.groupBy(a=>a)
    //对v进行操作
    val wordCount2: RDD[(String, Int)] = strRDD2.mapValues(a => a.size)

   // ③//扁平化
    val wordRDD3: RDD[String] = lineRDD.flatMap(a=>a.split(" "))
    //按key分组
    val strRDD3: RDD[(String, Iterable[String])] = wordRDD3.groupBy(a=>a)
    //模式匹配 (a,(1,1,1,1,1,)) -> (a,5)
    val wordCount3: RDD[(String, Int)] = strRDD3.map {
      case (word, list) => (word, list.size)
    }

   // ④//扁平化
    val wordRDD4: RDD[String] = lineRDD.flatMap(a=>a.split(" "))
    //结构转换
    val strRDD4: RDD[(String, Int)] = wordRDD4.map(a=>(a,1))
    //按key归一
    val wordCount4: RDD[(String, Int)] = strRDD4.reduceByKey((x, y)=>{x+y})


    //⑤//扁平化
    val wordRDD5: RDD[String] = lineRDD.flatMap(a=>a.split(" "))
    //结构转换
    val strRDD5: RDD[(String, Int)] = wordRDD5.map(a=>(a,1))
    //
    val wordCount5: RDD[(String, Int)] = strRDD5.foldByKey(0)((x, y)=>{x+y})

    //⑥
    //扁平化
    val wordRDD6: RDD[String] = lineRDD.flatMap(a=>a.split(" "))
    //结构转换
    val strRDD6: RDD[(String, Int)] = wordRDD6.map(a=>(a,1))
    //求和
    val comRDD: RDD[(String, (Int, Int))] = strRDD6.combineByKey((v: Int) => (v, 0),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    //转换结构
    val wordCount6: RDD[(String, Int)] = comRDD.map {
      case a => (a._1, a._2._1)
    }

    //⑦
    //扁平化
    val wordRDD7: RDD[String] = lineRDD.flatMap(a=>a.split(" "))
    //结构转换
    val strRDD7: RDD[(String, Int)] = wordRDD7.map(a=>(a,1))
    //分组
    val groRDD: RDD[(String, Iterable[Int])] = strRDD7.groupByKey()
    //结构转换
    val wordCount7: RDD[(String, Int)] = groRDD.map {
      case (word, datas) => (word, datas.sum)
    }


    //⑧
    //扁平化
    val wordRDD8: RDD[String] = lineRDD.flatMap(a=>a.split(" "))
    //结构转换
    val strRDD8: RDD[(String, Int)] = wordRDD8.map(a=>(a,1))

    val wordCount8: RDD[(String, Int)] = strRDD8.aggregateByKey(0)((x, y)=>{x+y}, (x, y)=>{x+y})

    wordCount8.collect().foreach(println)

    //rdd.map(t=>t._1*t._2).flatMap(c=>c.split("")).countByValue().foreach(println)

  }
}
