package com.info.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
//读取数据库中的数据
object Spark_Sql_JDBC {

  def main(args: Array[String]): Unit = {
    //配置环境
    val conf: SparkConf = new SparkConf().setAppName("Spark_Sql_JDBC").setMaster("local[*]")
    //创建SparkSql
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //读取数据库中的数据  通过JDBC
    val dF: DataFrame = spark.read
      .format("jdbc")
      .option("user", "root")
      .option("password", "root")
      .option("url", "jdbc:mysql://localhost:3306/frame?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true")
      .option("dbTable", "school")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .load()
    val selDF: DataFrame = dF.select("name")
    selDF.show()

    spark.stop()

  }
}
