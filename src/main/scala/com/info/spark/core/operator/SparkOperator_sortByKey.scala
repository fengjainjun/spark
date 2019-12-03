package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator_sortByKey {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val arrayRDD: RDD[(Int, String)] = sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
    //需求：创建一个pairRDD，按照key的正序和倒序进行排序
    val sortByKeyRDD: RDD[(Int, String)] = arrayRDD.sortByKey(true)
    val sortByKeyRDD1: RDD[(Int, String)] = arrayRDD.sortByKey(false)

    sortByKeyRDD.collect().foreach(println)
  }
}
