package it.jesinity.streaming.jobs

import it.jesinity.streaming.data._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object SimpleAvroStructuredStreamingJob {

  val messageSchema      = new Schema.Parser().parse(transactionSchema)
  val reader             = new GenericDatumReader[GenericRecord](messageSchema)
  val avroDecoderFactory = DecoderFactory.get()
  // Register implicit encoder for map operation
  implicit val encoder: Encoder[GenericRecord] = org.apache.spark.sql.Encoders.kryo[GenericRecord]

  def main(args: Array[String]): Unit = {

    //configure Spark
    val conf = new SparkConf().setAppName("kafka-structured").setMaster("local[*]")

    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, new Duration(2000))
    ssc.checkpoint(s"/tmp/uarauara${System.currentTimeMillis()}")

    //initialize spark session
    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", "3")

    import sparkSession.implicits._

    val dataset: DataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "transactions")
      .option("startingOffsets", "earliest")
      .load()
      .select($"value".as[Array[Byte]])
      .map { d =>
        val rec: GenericRecord = reader.read(null, avroDecoderFactory.binaryDecoder(d, null))
        transactionAvroFormat.from(rec)
      }.withColumn("sql_timestamp",to_timestamp($"datetime","yyyyMMddHHmmss"))


    //val value: Dataset[TransactionSmall] = dataset.withWatermark("datetime", "1 days")


    val frame: DataFrame = dataset
      .withWatermark("sql_timestamp", "30 seconds")
      .groupBy(
        $"user",
        $"amount",
        window($"sql_timestamp", "30 seconds", "15 seconds")
      ).count()

    //.groupBy($"user").count()

    frame.writeStream
      //.outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 200)
      // .trigger(Trigger.ProcessingTime(10.seconds))
      .start()

    //ssc.start()
    ssc.awaitTermination()
  }
}
