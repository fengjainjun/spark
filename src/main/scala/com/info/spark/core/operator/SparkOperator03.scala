package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator03 {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个glomrdd
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    //glom方法
    val glomRDD: RDD[Array[Int]] = listRDD.glom()
    glomRDD.collect().foreach(Array => {
      println(Array.mkString("-"))
    })
  }
}
