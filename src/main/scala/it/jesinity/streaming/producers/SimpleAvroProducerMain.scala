package it.jesinity.streaming.producers

import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat
import java.util.Properties

import com.twitter.bijection.avro.GenericAvroCodecs
import com.typesafe.scalalogging.StrictLogging
import it.jesinity.streaming.conf._
import it.jesinity.streaming.data._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

import com.sksamuel.avro4s.AvroOutputStream
import com.twitter.bijection.Injection

import scala.collection.JavaConverters._
import scala.collection.mutable

object SimpleAvroProducerMain extends KafkaProducerMain[String, Array[Byte], Unit] with StrictLogging {

  val USER_SCHEMA: String = transactionSchema

  def dateFormat_yyyyMMdd       = new SimpleDateFormat("yyyyMMdd")
  def dateFormat_yyyyMMddHHmmSS = new SimpleDateFormat("yyyyMMddHHmmss")

  override def customizeProperties(props: Properties): Unit = {
    props.put(`key.serializer`, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(`value.serializer`, "org.apache.kafka.common.serialization.ByteArraySerializer")
  }

  override def buildProducer(props: Properties): KafkaProducer[String, Array[Byte]] = {
    new KafkaProducer[String, Array[Byte]](props)
  }

  override def useProducer(producer: KafkaProducer[String, Array[Byte]]): Unit = {

    val parser: Schema.Parser                                  = new Schema.Parser
    val schema: Schema                                         = parser.parse(USER_SCHEMA)
    val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

    val day = ConfigHolder.configuration.getInt(`startingDay`)

    val userList: mutable.Seq[(String, Int)] = ConfigHolder.configuration.getStringList(users).asScala.zipWithIndex

    val startingDate = LocalDateTime.now() //LocalDateTime.of(2017, 1, 1, 0, 0)

    val topicTransactionName = ConfigHolder.configuration.getString(`topic.transaction`)

    (0 to 2) foreach { take =>
      userList.foreach { user =>
        val avroRecord: GenericData.Record = new GenericData.Record(schema)

        val time    = startingDate.plusSeconds(10*take)
        val newTime = time.plusSeconds(user._2)
        val zonedDateTime = newTime.atZone(ZoneId.systemDefault())
        val millis: Long =  zonedDateTime.toInstant.toEpochMilli
        val millisString = dateFormat_yyyyMMddHHmmSS.format(millis)

        avroRecord.put("user", user._1)
        avroRecord.put("userId", user._2)
        avroRecord.put("amount", randomInRange(2, 100))
        avroRecord.put("timestamp", millis)
        avroRecord.put("datetime", millisString)

        val bytes = recordInjection(avroRecord)

        val transaction = Transaction(user._1, user._2, randomInRange(2, 100), millis, millisString)

        logger.info(s"sending record... $transaction")

        val baos   = new ByteArrayOutputStream()
        val output = AvroOutputStream.binary[Transaction](baos)
        output.write(transaction)
        output.close()
        val newBytes = baos.toByteArray

        val record = new ProducerRecord[String, Array[Byte]](topicTransactionName, newBytes)

        producer.send(record)

      }
    }
  }

  def randomInRange(min: Int, max: Int): Double =
    min + Math.random() * (max - min)
}
