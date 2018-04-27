package it.jesinity.streaming.data

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time._
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.util.Date

import sun.java2d.pipe.SpanShapeRenderer.Simple

/**
  * created by CGnal s.p.a
  */
case class Transaction(user: String, userId: Int, amount: Double, timestamp: Long, datetime: String)

case class TransactionSmall(user: String, datetime: Timestamp)

object Ciccio {

  def main(args: Array[String]): Unit = {

    val l = System.currentTimeMillis()

    val date =
      LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneId.systemDefault())

    val time = date
      .atZone(ZoneId.systemDefault())
      .plusDays(4)
      .truncatedTo(ChronoUnit.DAYS)
      .toInstant()

    val dd = Date.from(time)

    println(dd)

    /*
    val date = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()).toLocalDate
    val time: LocalDateTime = date.atStartOfDay()
    val zoneId = ZoneId.systemDefault()
    Date.from(time.toInstant(zoneId)).getTime()
    println(time)*/

  }

  def boh = {}
}
