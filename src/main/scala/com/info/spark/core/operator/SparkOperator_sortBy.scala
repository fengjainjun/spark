package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator_sortBy {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD: RDD[Int] = sc.makeRDD(List(1,3,9,5,7,8))
    //按照自身排序
    val sortByRDD: RDD[Int] = listRDD.sortBy(x => x)
    sortByRDD.collect().foreach(println)

  }
}
