package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//TODO spark 读取hive数据
//hive-site.xml要放在resources下,集群模式就放在$SPARK_HOME/conf 下面
//bin/spark-shell  --jars mysql-connector-java-5.1.27-bin.jar
//权限不够: -DHADOOP_USER_NAME=xxxxxx （你的hadoop用户）
object TestHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
//    val sc = new SparkContext(conf)
//    val sqlContext = new HiveContext(sc)
//    sqlContext.sql("select * from spark0311.t_order").show()

    //val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath
    //println(warehouseLocation)
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use spark0311")
    spark.sql("select * from t_order").show()


  }
}
