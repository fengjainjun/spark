package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator08 {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16 ,4)
    println("缩减分区前="+listRDD.partitions.size)
    //缩减分区
    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
    println("缩减分区后="+coalesceRDD.partitions.size)
    //distinctRDD.collect().foreach(println)
  }
}