package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator_mapValues {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val arrayRDD: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")))
    //对value添加字符串"|||"
    val mapValueRDD: RDD[(Int, String)] = arrayRDD.mapValues(_+"|||")
    mapValueRDD.collect().foreach(println)
  }
}
