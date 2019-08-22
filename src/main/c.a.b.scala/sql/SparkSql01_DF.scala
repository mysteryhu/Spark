package sql


import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSql01_DF {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sql").setMaster("local[*]")
    //创建Spark SQL对象
    //SparkSession 私有的构造器 只能在本类和伴生对象中访问
    //val session: SparkSession = new SparkSession(conf)
    val session = SparkSession.builder().config(conf).getOrCreate()

    //创建dataframe
    val df: DataFrame = session.read.json("input/user.json")

    //创建表
    df.createOrReplaceTempView("user")

    //使用Sql方式来访问DF
    session.sql("select * from user").show()

    //使用DSL方式来访问DF
    df.select("name","age").show()
    //关闭spark
    session.stop()
  }
}
