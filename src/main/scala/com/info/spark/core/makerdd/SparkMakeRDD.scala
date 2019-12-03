package com.info.spark.core.makerdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkMakeRDD {

  def main(args: Array[String]): Unit = {
    //创建sparkconfig
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkMakeRDD")
    //创建sparkcontext
    val sc: SparkContext = new SparkContext(conf)

    //make创建RDD
    val arrRDD: RDD[Int] = sc.makeRDD(Array(1,2,3,4))
    //parallelize创建RDD
    val listRDD: RDD[String] = sc.parallelize(List("1","2","3","4"))

    //arrRDD.collect().foreach(println)
    listRDD.collect().foreach(println)

  }
}
