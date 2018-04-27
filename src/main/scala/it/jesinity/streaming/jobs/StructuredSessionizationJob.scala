package it.jesinity.streaming.jobs

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming._

/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network.
  *
  * Usage: MapGroupsWithState <hostname> <port>
  * <hostname> and <port> describe the TCP server that Structured Streaming
  * would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  * `$ nc -lk 9999`
  * and then run the example
  * `$ bin/run-example sql.streaming.StructuredSessionization
  * localhost 9999`
  */
object StructuredSessionizationJob {

  def main(args: Array[String]): Unit = {

    import it.jesinity.streaming.conf.ConfigHolder.configuration
    import it.jesinity.streaming.conf._

    val boostrapServer = configuration.getString(`bootstrap.servers`)
    val topic          = configuration.getString(`topic.wordcount`)

    val spark = SparkSession.builder
      .appName("StructuredSessionization")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val events: Dataset[Event] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", boostrapServer)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)]
      .flatMap {
        case (line, timestamp) =>
          line.split(" ").map(word => Event(sessionId = word, timestamp))
      }

    var anotherRDD: RDD[Int] = spark.sparkContext.parallelize(1 to 10)

    // Sessionize the events. Track number of events, start and end timestamps of session, and
    // and report session updates.
    val sessionUpdates: Dataset[SessionUpdate] = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

        case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>

          val value = spark.sparkContext.parallelize(1 to 10)

          anotherRDD = anotherRDD ++ value

          // If timed out, then remove session and send final update
          if (state.hasTimedOut) {
            val finalUpdate =
              SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
            state.remove()
            finalUpdate
          } else {
            // Update start and end timestamps in session
            val timestamps = events.map(_.timestamp.getTime).toSeq
            val updatedSession = if (state.exists) {
              val oldSession: SessionInfo = state.get
              SessionInfo(oldSession.numEvents + timestamps.size, oldSession.startTimestampMs, math.max(oldSession.endTimestampMs, timestamps.max))
            } else {
              SessionInfo(timestamps.size, timestamps.min, timestamps.max)
            }
            state.update(updatedSession)

            // Set timeout such that the session will be expired if no data received for 10 seconds
            state.setTimeoutDuration("10 seconds")
            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
          }
      }

    // Start running the query that prints the session updates to the console
    val query = sessionUpdates.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

/** User-defined data type representing the input events */
case class Event(sessionId: String, timestamp: Timestamp)

/**
  * User-defined data type for storing a session information as state in mapGroupsWithState.
  *
  * @param numEvents        total number of events received in the session
  * @param startTimestampMs timestamp of first event received in the session when it started
  * @param endTimestampMs   timestamp of last event received in the session before it expired
  */
case class SessionInfo(numEvents: Int, startTimestampMs: Long, endTimestampMs: Long) {

  /** Duration of the session, between the first and last events */
  def durationMs: Long = endTimestampMs - startTimestampMs
}

/**
  * User-defined data type representing the update information returned by mapGroupsWithState.
  *
  * @param id          Id of the session
  * @param durationMs  Duration the session was active, that is, from first event to its expiry
  * @param numEvents   Number of events received by the session while it was active
  * @param expired     Is the session active or expired
  */
case class SessionUpdate(id: String, durationMs: Long, numEvents: Int, expired: Boolean)

// scalastyle:on println
