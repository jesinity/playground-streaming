package it.jesinity.streaming

import com.sksamuel.avro4s.RecordFormat

import scala.io.Source

package object data {

  def transactionSchemaString(schemaName: String) : String = {
    val stream = this.getClass.getResourceAsStream(schemaName)
    Source.fromInputStream(stream).getLines().mkString(" ")
  }

  //lazy val transactionSchema = transactionSchemaString("transaction.avsc")
  lazy val transactionSchema =
    """
      |{
      |  "namespace": "it.jesinity.streaming.data",
      |  "type": "record",
      |  "name": "Transaction",
      |  "fields": [
      |    {
      |      "name": "user",
      |      "type": "string"
      |    },
      |    {
      |      "name": "userId",
      |      "type": "int"
      |    },
      |    {
      |      "name": "amount",
      |      "type": "double"
      |    },
      |    {
      |      "name": "timestamp",
      |      "type": "long"
      |    },
      |    {
      |      "name": "datetime",
      |      "type": "string"
      |    }
      |  ]
      |}
    """.stripMargin


  val transactionAvroFormat = RecordFormat[Transaction]

}