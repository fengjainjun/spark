package com.info.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark_Sql_Transform {

  def main(args: Array[String]): Unit = {
    //1.编写sparksql的conf
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Sql_Demo").setMaster("local[*]")

    //2.创建sparksql
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //导入隐式转换，使得RDD可转换为DataFrame
    import spark.implicits._
    //3.创建RDD
    val listRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 10), (2, "lisi", 20), (3, "wangwu", 30)))
    //4.转化成DF
    val df: DataFrame = listRDD.toDF("id", "name", "age")
    //5.转化成DS
    val ds: Dataset[User] = df.as[User]
    //6.转化成DF
    val df1: DataFrame = ds.toDF()
    //6.转化成RDD
    val rdd: RDD[Row] = df1.rdd
    //7.遍历数据
    rdd.foreach{
      row =>
        row.getString(1).foreach(println)
    }
    //5.释放资源
    spark.stop()
  }
}

case class User(id: Int, name: String, age: Int)
