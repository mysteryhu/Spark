package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark04_WordCount$ {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //从集合*(内存)中创建RDD
    //方法的第二个参数表示执行并行度(分区数)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    //保存数据
    rdd.saveAsTextFile("output")

    //释放资源
    sc.stop()

  }
}
