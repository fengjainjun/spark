package com.info.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparingStreaming_WordCount01 {

  def main(args: Array[String]): Unit = {
    /*
    使用sparkstreaming实时的读取数据
    */
    //0.编写配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparingStreaming_WordCount01")
    //1.创建sparkstreaming对象
    val sc: StreamingContext = new StreamingContext(conf,Seconds(5))
    //实时的读取目录下的文件
    val fileDStream: DStream[String] = sc.textFileStream("in/")
    //读取到的文件的内容进行切割
    val flatMapDStream: DStream[String] = fileDStream.flatMap(_.split(" "))
    //对切割的字段进行数据结构的改变
    val mapDStream: DStream[(String, Int)] = flatMapDStream.map((_,1))
    //相同的key进行聚合
    val reduceByKeyDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)
    //打印结果
    reduceByKeyDStream.print( )
    //启动sparkstreaming
    sc.start()
    //等待结果
    sc.awaitTermination()
  }
}
