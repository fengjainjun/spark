package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator_zip {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD1: RDD[Int] = sc.makeRDD(1 to 3)
    val listRDD2: RDD[String] = sc.makeRDD(List("a","b","c"))
    //需求：创建两个RDD，并将两个RDD组合到一起形成一个(k,v)RDD
    val zipRDD: RDD[(Int, String)] = listRDD1.zip(listRDD2)

    zipRDD.collect().foreach(println)
  }
}
