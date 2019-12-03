package com.info.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark_Sql_Demo {

  def main(args: Array[String]): Unit = {
    //1.编写sparksql的conf
    val sparkConf : SparkConf = new SparkConf().setAppName("Spark_Sql_Demo") .setMaster("local[*]")

    //2.创建sparksql
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //3.读取数据
    val df: DataFrame = spark.read.json("in/user.json")
    //4.展示
    df.show()
    //5.释放资源
    spark.stop()
  }
}
