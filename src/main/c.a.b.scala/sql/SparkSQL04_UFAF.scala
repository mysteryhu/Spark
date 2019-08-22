package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._


object SparkSQL04_UFAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sql").setMaster("local[*]")
    //创建Spark SQL 对象
    val session = SparkSession.builder().config(conf).getOrCreate()

    //创建自定义聚合函数
    val ageAvg = new AgeAvgStrongFunction


    val rdd: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1,"ls",20),(2,"ww",30)))
    //rdd -> ds
    import session.implicits._
    //使用DSL语法来访问强类型聚合函数

    val empXRDD: RDD[EmpX] = rdd.map {
      case (id, name, age) => EmpX(id, name, age)
    }
    val dsEmpX: Dataset[EmpX] = empXRDD.toDS()
    //将聚合函数转换为查询的列
    val avgAge: TypedColumn[EmpX, Double] = ageAvg.toColumn.name("avgAge")

    val res: Dataset[Double] = dsEmpX.select(avgAge)
    println(res)



  }
}

case class EmpX( var id:Long, var name:String, var age:Long)
case class AvgBuffer(var sum:Long,var count:Long)
//自定义聚合函数(强类型)
//1.继承并声明泛型
//2.重写
class AgeAvgStrongFunction extends Aggregator[EmpX,AvgBuffer,Double]{
  //缓冲区初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0L,0L)
  }
  //更新缓冲区
  override def reduce(buff: AvgBuffer, emp: EmpX): AvgBuffer = {
    buff.sum=buff.sum+emp.age
    buff.count=buff.count+1L
    buff

  }
  //合并缓冲区
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.count=b1.count+b2.count
    b1.sum=b1.sum+b2.sum
    b1

  }
  //最终的计算
  override def finish(reduction: AvgBuffer): Double ={
    reduction.sum.toDouble/reduction.count
  }
  override def bufferEncoder: Encoder[AvgBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}