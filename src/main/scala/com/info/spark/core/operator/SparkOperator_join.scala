package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator_join {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //需求：创建两个pairRDD，并将key相同的数据聚合到一个元组。
    val arrayRDD1: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c")))
    val arrayRDD2: RDD[(Int, Int)] = sc.parallelize((Array((1,4),(2,5),(3,6))))

    val joinRDD: RDD[(Int, (String, Int))] = arrayRDD1.join(arrayRDD2)
    joinRDD.collect().foreach(println)
  }
}
