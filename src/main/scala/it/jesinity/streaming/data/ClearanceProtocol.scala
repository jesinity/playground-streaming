package it.jesinity.streaming.data

import spray.json.DefaultJsonProtocol

/**
  * spray json protocol
  */
object ClearanceProtocol extends DefaultJsonProtocol {

  implicit val clearanceJsonProtocol = jsonFormat3(Clearance)

}
