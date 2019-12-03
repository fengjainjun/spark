package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator07 {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,5,4,2,1,7,1,2,9,8,1,2))
    //对数据进行去重
    val distinctRDD: RDD[Int] = listRDD.distinct()
    distinctRDD.collect().foreach(println)
  }
}