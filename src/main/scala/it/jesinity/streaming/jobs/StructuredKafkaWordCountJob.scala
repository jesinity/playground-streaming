package it.jesinity.streaming.jobs

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.window

import scala.collection.immutable

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: StructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
  *     [<checkpoint-location>]
  *   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
  *   comma-separated list of host:port.
  *   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
  *   'subscribePattern'.
  *   |- <assign> Specific TopicPartitions to consume. Json string
  *   |  {"topicA":[0,1],"topicB":[2,4]}.
  *   |- <subscribe> The topic list to subscribe. A comma-separated list of
  *   |  topics.
  *   |- <subscribePattern> The pattern used to subscribe to topic(s).
  *   |  Java regex string.
  *   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
  *   |  specified for Kafka source.
  *   <topics> Different value format depends on the value of 'subscribe-type'.
  *   <checkpoint-location> Directory in which to create checkpoints. If not
  *   provided, defaults to a randomized directory in /tmp.
  *
  * Example:
  *    `$ bin/run-example \
  *      sql.streaming.StructuredKafkaWordCount host1:port1,host2:port2 \
  *      subscribe topic1,topic2`
  */
object StructuredKafkaWordCountJob {
  def main(args: Array[String]): Unit = {

    import it.jesinity.streaming.conf._
    import it.jesinity.streaming.conf.ConfigHolder.configuration

    val boostrapServer = configuration.getString(`bootstrap.servers`)
    val topic          = configuration.getString(`topic.wordcount`)

    val conf = new SparkConf().setAppName("structured-kafka-wordcount").setMaster("local[*]")

    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark: SparkSession = SparkSession.builder
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val batchDataset = ("cgnal",new Timestamp(System.currentTimeMillis())) :: ("davide",new Timestamp(System.currentTimeMillis()))  :: Nil

    val batchDataFrame = spark.sparkContext.parallelize(batchDataset).toDF("value_batch","timestamp_batch")

        // Create DataSet representing the stream of input lines from kafka
    val lines: Dataset[(String, Timestamp)] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", boostrapServer)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)]

    val joined = lines.join(batchDataFrame,$"value" ===$"value_batch","fullouter" ).select(
      coalesce($"value", $"value_batch").alias("value"), coalesce($"timestamp", $"timestamp_batch").alias("timestamp")
    ).as[(String,Timestamp)]


    // Generate running word count
    val wordCounts: DataFrame = joined
      .flatMap { x =>
        x._1.split(" ").map { y =>
          WordAndTime(y, x._2)
        }
      }
      .withWatermark("timestamp", "15 seconds")
      .groupBy(
        window($"timestamp", "30 seconds", "30 seconds"),
        $"word",
        $"timestamp"//,
      //
      )
      .count()

    /*.groupBy(
        $"value",
        window($"timestamp", "30 seconds", "15 seconds")
      ).count()*/

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 200)
    //  .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }

}

case class WordAndTime(word: String, timestamp: Timestamp)
