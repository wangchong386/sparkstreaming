package sparkstreaming

import kafka.message.MessageAndMetadata
import org.apache.commons.codec.Decoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import scala.reflect.ClassTag

/**
  * Created by wangchong on 2017/6/22.
  */
object createDirectStream {
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag] (
    ssc: StreamingContext,
    kafkaParams: Map[String, String],
    topics: Set[String]
  ): InputDStream[(K, V)] = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
    val kc = new KafkaCluster(kafkaParams)
    val fromOffsets = getFromOffsets(kc, kafkaParams, topics)
    new DirectKafkaInputDStream[K, V, KD, VD, (K, V)](
      ssc, kafkaParams, fromOffsets, messageHandler)
}
