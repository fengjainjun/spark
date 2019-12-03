package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator_reduceByKey {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("female",1),("male",5),("female",5),("male",2)))
    // 需求：创建一个pairRDD，计算相同key对应值的相加结果
    val reduceByKeyRDD: RDD[(String, Int)] = listRDD.reduceByKey {
      (x, y) =>
        (x + y)
    }
    reduceByKeyRDD.collect().foreach(println)
  }
}
