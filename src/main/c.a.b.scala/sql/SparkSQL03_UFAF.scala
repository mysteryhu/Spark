package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQL03_UFAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sql").setMaster("local[*]")
    //创建Spark SQL 对象
    val session = SparkSession.builder().config(conf).getOrCreate()

    //用户自定义聚合函数(UDAF)
    //select avg(age) from user


    //创建自定义聚合函数
    val ageAvg = new AgeAvgFunction

    //向Spark进行注册
    session.udf.register("avgAge",ageAvg)
    val rdd: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1,"ls",20),(2,"ww",30)))

    import session.implicits._
    val df: DataFrame = rdd.toDF("id","name","age")
    df.createOrReplaceTempView("user")

    session.sql("select avgAge(age) from user").show()



  }
}


//自定义聚合函数
//1.继承
//2.重写
class AgeAvgFunction extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    //输入数据的结构类型
    new StructType().add("age",LongType)
  }
    //缓冲区的数据结构类型
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }
    //函数的返回结果类型
  override def dataType: DataType = {
    DoubleType
  }
    //函数稳定性
  override def deterministic: Boolean = {
    true
  }
    //缓冲区数据初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }
    //更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getLong(0)+input.getLong(0)
    buffer(1)=buffer.getLong(1)+1L
  }
    //分布式的理念,合并多个buffer
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)

  }
    //计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }

}