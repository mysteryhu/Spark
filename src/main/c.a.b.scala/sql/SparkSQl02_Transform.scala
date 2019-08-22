package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQl02_Transform {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sql").setMaster("local[*]")
    val session = SparkSession.builder().config(conf).getOrCreate()

    //RDD DF DS

    val rdd1: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1,"zs",20),(2,"ww",30)))

    //TODO 将RDD转换成DF
    //RDD转换为Df,DS时,需要增加隐式转换,需要引入spark环境对象的隐式转换规则
    import session.implicits._
    /*
    val df: DataFrame = rdd.toDF("id","name","age")
    //df.show()

    //TODO 将DataFrame转换成RDD
    val rdd1: RDD[Row] = df.rdd

    rdd1.foreach(row=>{
      //row的查询,基于索引
      println(row.getInt(0)+row.getString(1)+row.getInt(2))
      println(row.get(0))
    })
    */
    //TODO 将DF转换成DS
    /*
    val df: DataFrame = rdd.toDF("id","name","age" )
    val ds: Dataset[User] = df.as[User]
    ds.show()

    //TODO DS ->DF
    val df1: DataFrame = ds.toDF()
    df1.show()
    */

    //TODO 将RDD转换为DataSet
    val userRDD: RDD[User] = rdd1.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()
    userDS.show()

    //TODO DS -> RDD
    val rdd2: RDD[User] = userDS.rdd
    rdd2.foreach(println)
    //关闭spark
  }
}


case class User(id:Int,name:String,age:Int)