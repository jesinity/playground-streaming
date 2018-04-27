package it.jesinity.streaming.data

import spray.json.DefaultJsonProtocol

/**
  * created by CGnal s.p.a
  */
object ClearanceProtocol extends DefaultJsonProtocol {

  implicit val clearanceJsonProtocol = jsonFormat3(Clearance)

}
