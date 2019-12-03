package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator06 {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,2)
    //对数据进行过滤
    val samleRDD: RDD[Int] = listRDD.sample(false,0.1,1)
    samleRDD.collect().foreach(println)
  }
}