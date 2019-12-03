package com.info.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

object Spark_Sql_UDF {
  def main(args: Array[String]): Unit = {
    /**
      * 自定义函数，在sql中使用
      */
    //设置spark环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Sql_UDF")
    //构建sparksession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //执行语句
    val df: DataFrame = spark.read.json("")
    //创建临时表
    //df.createOrReplaceTempView("user")
    //转化为ds
    val ds: Dataset[UserBean] = df.as[UserBean]
    //用sparksql进行查询
    //spark.sql("select * from user")
    //
    spark.udf.register("avg",AvgAge)
    ds.select("age")
    //
  }
}
case class UserBean(id : Int,name : String,age:Long)
case class AvgAge(var sum : Long,var count : Int)
//编写自定义方法类
object MyAvg extends  Aggregator[UserBean,AvgAge,Double] {
  //对数据进行初始化操作
  override def zero: AvgAge = {
    new AvgAge(0L,0)
  }
  //对数据进行聚合
  override def reduce(b: AvgAge, a: UserBean): AvgAge ={
      b.sum += a.age
      b.count += 1
      b
  }
  //不同的executor计算的结果进行合并
  override def merge(b1: AvgAge, b2: AvgAge): AvgAge = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }
  //最终计算的结构
  override def finish(reduction: AvgAge): Double = {
    reduction.sum.toDouble / reduction.count
  }
  // 设定之间值类型的编码器，要转换成case类
  override def bufferEncoder: Encoder[AvgAge] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}

