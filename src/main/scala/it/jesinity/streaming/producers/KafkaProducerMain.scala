package it.jesinity.streaming.producers

import java.util.Properties

import com.twitter.bijection.avro.GenericAvroCodecs
import com.typesafe.scalalogging.StrictLogging
import it.jesinity.streaming.conf.ConfigHolder.configuration
import it.jesinity.streaming.conf.{`bootstrap.servers`, `key.serializer`, `value.serializer`}
import it.jesinity.streaming.producers.SimpleAvroProducerMain.USER_SCHEMA
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.control.NonFatal

/**
  * created by CGnal s.p.a
  */
trait KafkaProducerMain[K, V, R] {

  self: StrictLogging =>

  def customizeProperties(properties: Properties): Unit

  def buildProducer(properties: Properties): KafkaProducer[K, V]

  def useProducer(producer: KafkaProducer[K, V]): R

  @throws[InterruptedException]
  def main(args: Array[String]): Unit = {

    val props = new Properties

    props.put(`bootstrap.servers`, configuration.getString(`bootstrap.servers`))

    customizeProperties(props)

    logger.info(s"bootstrapping Kafka producer with properties\n $props")

    val prod: KafkaProducer[K, V] = buildProducer(props)

    try {
      val result = useProducer(prod)
      logger.info(s"obtaining result:\n $result")
    } catch {
      case NonFatal(e) =>
        logger.error("unexpected error", e)
    } finally {
      logger.info("closing producer")
      prod.close()
    }
  }
}
