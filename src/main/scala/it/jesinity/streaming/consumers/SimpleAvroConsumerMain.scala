package it.jesinity.streaming.consumers

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import it.jesinity.streaming.data._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

import scala.util.Try


/**
  * An example on how spark consumes avro record.
  *
  */
object SparkAvroConsumer {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, new Duration(2000))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "transactionsId",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("transactions")
    val directKafkaStream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      PreferConsistent,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    )

    val transactionSchemaBroadcast = sc.broadcast(transactionSchema)

    directKafkaStream.foreachRDD { rdd =>

      rdd.foreach { avroRecord =>

        val parser = new Schema.Parser
        val schema = parser.parse(transactionSchemaBroadcast.value)
        val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
        val record: GenericRecord = recordInjection.invert(avroRecord.value()).get
        val transaction: Transaction = transactionAvroFormat.from(record)
        println(transaction)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}