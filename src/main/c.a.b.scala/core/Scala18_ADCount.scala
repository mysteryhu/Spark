package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Scala18_ADCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //需求:统计每个省份广告被点击次数的top3

    //ToDo 1 .从日志中获取用户的广告点击数据
    val textRDD: RDD[String] = sc.textFile("input/agent.log")
    //ToDo 2. 将数据转换结构 (prv -adv ,1)
    val mapRDD: RDD[(String, Int)] = textRDD.map(line => {
      //1516609240717 6 9 32 6
      //时间戳 省份 城市 用户 广告
      //切分数据
      val datas: Array[String] = line.split(" ")
      (datas(1) + "-" + datas(4), 1)
    })

    //TODO 3. 将转换结构后的数据进行聚合统计 (prv -adv,1)=>(prv,adv,sum)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    //TODO 4.将聚合的结果进行结构的转换: (prv -adv,sum)=>(prv,(adv,sum))
    val mapRDD1: RDD[(String, (String, Int))] = reduceRDD.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("-")
        (keys(0), (keys(1), sum))
      }
    }
    //TODO 5.将结构转换后的数据按照省份进行分组 (prv,Iterator[(adv,sum)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()

    //TODO 6. 将分组后的数据进行排序 降序 取前三名
    val adRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
      datas.toList.sortWith {
        (left, right) => {
          left._2 > right._2
        }
      }.take(3)
    })
    adRDD.collect().foreach(println)
    //释放资源
  }
}
