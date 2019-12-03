package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator01 {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //改变数据结构，相当于数据的批处理
    /*val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(_ * 2)
    })*/
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(_.map(_ * 2))
    //打印
    mapPartitionsRDD.collect().foreach(println)
  }
}
