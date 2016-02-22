package com.scb.msg.processor
import com.prowidesoftware.swift.model.mt.mt1xx.MT103
import com.prowidesoftware.swift.model.mt.AbstractMT
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.log4j.Logger

case class Swift(sender_name: String, sender_bic: String, sender_country: String, beneficiary_name: String, beneficiary_bic: String, beneficiary_country: String, currency: String, amount: String, purpose: String, payment_direction: String)

object Swift {
  val logger = Logger.getLogger(this.getClass())

  def convertSwift(swift: String): Swift = {
    val mt103: MT103 = MT103.parse(swift.replaceAll("\t", "\n"))
    Swift(safeStr(mt103.getSender()),
      safeStr(mt103.getField53A().getBIC()),
      safeStr(mt103.getField53D().getNameAndAddress()),
      safeStr(mt103.getField59().getNameAndAddress()),
      safeStr(mt103.getField57A().getBIC()),
      safeStr(mt103.getField59F().getNameAndAddress1()),
      safeStr(mt103.getField32A().getAmount()),
      safeStr(mt103.getField32A().getCurrency()),
      safeStr(mt103.getField70().getComponent1()),
      getDirection(mt103))
  }

  def isMT103(swift: String) = {
    try {
      var message: AbstractMT = AbstractMT.parse(swift.replaceAll("\t", "\n"))
      isValidMT103Message(message)
    } catch {
      case t: Throwable =>
        logger.error(" Unknown message or file format " + ExceptionUtils.getStackTrace(t));
        false
    }
  }

  private def isValidMT103Message(message: com.prowidesoftware.swift.model.mt.AbstractMT) = {
    message != null && message.isType(103)
  }

  private def getDirection(mt103: com.prowidesoftware.swift.model.mt.mt1xx.MT103) = {
    val direction = if (mt103.isOutput()) "Output" else "Input"
    direction
  }
  private def safeStr(str: Object) = {
    try {
      str.toString()
    } catch {
      case t: Throwable => " ";
    }
  }

  private def concat(ss: String*) = ss filter (_.nonEmpty) mkString "\t "
}
