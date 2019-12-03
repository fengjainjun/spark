package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator04 {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    //对数据进行分组
    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(x => x%2)
    groupByRDD.collect().foreach(println)

  }
}