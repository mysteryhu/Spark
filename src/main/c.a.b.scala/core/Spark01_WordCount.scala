package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //使用Spark框架完成第一个案例


    //使用Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //获取Spark上下文对象
    val sc = new SparkContext(conf)

    //从当前环境中获取文件
    //读取文件内容,形成一行行的字符串
    val lineRdd: RDD[String] = sc.textFile("input")

    //将一行行的字符串切分成一个个的单词
    val wordRDD: RDD[String] = lineRdd.flatMap(line=>line.split(" "))

    //将单词进行结构的转变, word=>(word,1) 为了统计的方便
    val wordToOneRdd: RDD[(String, Int)] = wordRDD.map(word=>(word,1))

    //使用Spark特殊的聚合方法完成单词数量的统计
    //wordToOneRDD.reduceByKey((x,y)=>{x+y})
    val wordToSumRDD: RDD[(String, Int)] = wordToOneRdd.reduceByKey((x,y)=>x+y)

    //将统计的结果采集出来
    val results: Array[(String, Int)] = wordToSumRDD.collect()
    //将结果打印在控制台上
    results.foreach(println)
    //释放资源
    sc.stop()
  }
}
