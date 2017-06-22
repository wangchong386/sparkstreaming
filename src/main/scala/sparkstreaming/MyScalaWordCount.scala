package sparkstreaming

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangchong on 2017/6/22.
  */
object MyScalaWordCount {
  def main(args: Array[String]): Unit = {
    //参数检查
    if (args.length < 2) {
      System.err.println("Usage: MyScalaWordCout <input> <output> ")
      System.exit(1)
    }
    //获取参数
    val input = args(0)
    val output = args(1)
    //创建scala版本的SparkContext
    val conf = new SparkConf().setAppName("MyScalaWordCout ")
    val sc = new SparkContext(conf)
    //读取数据
    val lines = sc.textFile(input)
    //进行相关计算
    val resultRdd = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //保存结果
    resultRdd.saveAsTextFile(output)
    sc.stop()
  }
}