package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator_subtract {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD1: RDD[Int] = sc.makeRDD(1 to 10)
    val listRDD2: RDD[Int] = sc.makeRDD(3 to 8)
    //需求：创建两个RDD，求第一个RDD与第二个RDD的差集
    val subtractRDD: RDD[Int] = listRDD1.subtract(listRDD2)

    subtractRDD.collect().foreach(println)
  }
}
