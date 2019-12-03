package com.info.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.immutable.StringOps
import scala.collection.mutable
//读取数据，用自己的自定义分区
object WordCount {

  def main(args: Array[String]): Unit = {
    //配置spark环境
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //创建一个 SparkContext
    val sc = new SparkContext(conf)
    //读取文件
    val fileRDD: RDD[String] = sc.textFile("in/page.log")
    //改变数据结构
    val mapRDD: RDD[((String, String), Int)] = fileRDD.map {
      case lines =>
        val line: String = lines.replaceAll("http://", " ")
        val strings:Array[String] = line.split("/")
        val word:Array[String] = strings(0).split("\\.")
        val zy: String = word(0)
        val name: String = strings(1)
        ((zy, name), 1)
    }
//    val value: RDD[String] = mapRDD.map()
    //聚合、排序、改变结构
    val reduceByKeyRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey((x,y) => x+y)
    val sortByRDD: RDD[((String, String), Int)] = reduceByKeyRDD.sortBy(_._2,false)
    val mappRDD: RDD[(String, (String, Int))] = sortByRDD.map {
      case lines =>
        val zy: String = lines._1._1
        val name: String = lines._1._2
        val num: Int = lines._2
        (zy, (name, num))
    }
//    val partitionByRDD: RDD[((String, String), Int)] = reduceByKeyRDD.partitionBy(new MyPartition(3))
    //自定义分区调用
     val partitionByRDD: RDD[(String, (String, Int))] = mappRDD.partitionBy(new MyPartition(4))
    partitionByRDD.saveAsTextFile("out")
  }
}
//自定义分区
class MyPartition(num : Int ) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
   /*val i: Int = key.toString.length
    i % num
*/
      if (key.toString.equals(" bigdata")){
        1
      }else if(key.toString.equals(" javaee")){
        2
      }else{
        3
      }
    }
}
