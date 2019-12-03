package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator_intersection{
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD1: RDD[Int] = sc.makeRDD(1 to 10)
    val listRDD2: RDD[Int] = sc.makeRDD(5 to 15)
    //需求：创建两个RDD，求两个RDD的交集
    val unionRDD: RDD[Int] = listRDD1.intersection(listRDD2)

    unionRDD.collect().foreach(println)
  }
}
