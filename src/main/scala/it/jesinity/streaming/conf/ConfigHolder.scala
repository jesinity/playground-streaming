package it.jesinity.streaming.conf

import com.typesafe.config.ConfigFactory

/**
  * configuration holder
  */
object ConfigHolder {

  val configuration = ConfigFactory.load()

}
