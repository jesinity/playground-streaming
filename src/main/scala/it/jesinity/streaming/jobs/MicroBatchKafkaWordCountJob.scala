package it.jesinity.streaming.jobs

import java.sql.Timestamp
import java.util.UUID

import it.jesinity.streaming.data.Clearance
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import spray.json._
import it.jesinity.streaming.data.ClearanceProtocol._
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature._

import scala.collection.immutable

object MicroBatchKafkaWordCountJob {

  def main(args: Array[String]): Unit = {

    import it.jesinity.streaming.conf._
    import it.jesinity.streaming.conf.ConfigHolder.configuration

    val topic          = configuration.getString(`topic.wordcount`)

    val conf = new SparkConf().setAppName("micro-batch-kafka-wordcount").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder
      .config(conf)
      .getOrCreate()

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(10))
    streamingContext.checkpoint(s"/tmp/uarauara${System.currentTimeMillis()}")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val kafkaParams = Map[String, Object](
      "bootstrap.servers"  -> "localhost:9092",
      "key.deserializer"   -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id"           -> "group1",
      "auto.offset.reset"  -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val stringStream: DStream[String]                     = stream.map(_.value())
    val clearanceStream: DStream[Clearance]               = stringStream.map(_.parseJson.convertTo[Clearance])
    val userClearanceStream: DStream[(String, Clearance)] = clearanceStream.map(clearance => (clearance.user, clearance))

    val streamingCombinedStream: MapWithStateDStream[String, Clearance, UserActivity, (String, UserActivity)] = userClearanceStream.mapWithState(
      StateSpec.function(MappingFunction.mappingFunc)
    )

    streamingCombinedStream.foreachRDD { rdd =>
      val clearances: RDD[Clearance] = rdd.flatMap(_._2.transactions)
      clearances.toDF().show(100)

    }

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}

object MappingFunction {

  // Update the cumulative count using mapWithState
  // This will give a DStream made of state (which is the cumulative count of the words)
  val mappingFunc2 = (word: String, one: Option[Int], state: State[Int]) => {
    val sum                   = one.getOrElse(0) + state.getOption.getOrElse(0)
    val output: (String, Int) = (word, sum)
    state.update(sum)
    output
  }

  val mappingFunc = (userId: String, one: Option[Clearance], state: State[UserActivity]) => {

    one match {
      case Some(cl) =>
        val newState = state.getOption match {
          case Some(UserActivity(trans)) => UserActivity(cl :: trans)
          case None                      => UserActivity(cl :: Nil)
        }
        state.update(newState)
        (userId, newState)
      case _ =>
        (userId, UserActivity(Nil))
    }
  }
}

case class UserActivity(transactions: List[Clearance])
