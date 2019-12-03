package com.info.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark_Sql_JDBC_Save {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Sql_JDBC_Save")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import sparkSession.implicits._

    val arrayRDD: RDD[(Int, String, Int)] = sparkSession.sparkContext.makeRDD(Array((1,"zhangsan",10),(2,"lisi",20),(3,"wangwu",30)))
    val frame: DataFrame = arrayRDD.toDF("id","name","age")
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","root")
    properties.put("driver","com.mysql.cj.jdbc.Driver")
    frame.write.jdbc(
      "jdbc:mysql://localhost:3306/frame?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true",
      "person",
      properties
    )

  }
}
