package it.jesinity.streaming.data


/**
  * a transaction activity
  * @param transactionId
  * @param user
  * @param amount
  */
case class Clearance(transactionId: String, user:String, amount:Double)


