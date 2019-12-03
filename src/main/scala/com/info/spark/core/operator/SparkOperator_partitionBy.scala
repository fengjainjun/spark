package com.info.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SparkOperator_partitionBy {
  def main(args: Array[String]): Unit = {
    //获取sparkkconf
    val conf: SparkConf = new SparkConf().setAppName("SparkOperator").setMaster("local[*]")
    //获取sparkcontext
    val sc = new SparkContext(conf)
    //创建一个rdd
    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)
    //需求：创建一个4个分区的RDD，对其重新分区

    println(listRDD.partitions.size)
    val partitionByRDD: RDD[(Int, String)] = listRDD.partitionBy(new HashPartitioner(2))
    println(partitionByRDD.partitions.size)

  }
}
