package sparkstreaming

import org.apache.commons.codec.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
/**
  * Created by wangchong on 2017/6/22.
  */
object DirectKafkaSparkStreaming {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <hdfspath> is a HDFS Path, like /user/admin/scalapath
        """.stripMargin)
      System.exit(1)
}
    val Array(brokers, topics, groupId, hdfsPath) = args
    //AppName参数是application的名字，展现在yarn集群web界面
    val sparkConf = new SparkConf().setAppName("DirectKafkaSparkStreaming")
    //创建StreamingContext，并配置 batch间隔20S
    val ssc = new StreamingContext(sparkConf, Seconds(20))

    //直连方式不支持还原点机制，需要自己记录偏移量
    val topicsSet = topics.split(",").toSet
    //Return a new DStream by applying a function to all elements of this DStream
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
}
}