package core

import org.apache.spark.{SparkConf, SparkContext}


object Scala23_RDD_CheckPoint {
  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    //Spark检查点一般存储在HDFS上
    sc.setCheckpointDir("cp")

  }
}
