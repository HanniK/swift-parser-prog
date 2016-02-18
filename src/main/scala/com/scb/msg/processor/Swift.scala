package com.scb.msg.processor
import com.prowidesoftware.swift.model.mt.mt1xx.MT103
import com.prowidesoftware.swift.model.mt.AbstractMT
import org.apache.commons.lang.exception.ExceptionUtils

case class Swift(msg_detail: String)

object Swift {

  def convertSwift(swift: String): String = {
    println(swift)
    var message: AbstractMT = null
    var processedStr: String = ""
    try {
      message = AbstractMT.parse(swift.replaceAll("\t", "\n"))
      if (isValidMT103Message(message)) {
        val mt103: MT103 = message.asInstanceOf[MT103]
        println(mt103)
        processedStr = parseMessageDetail(mt103, getDirection(mt103))
      }
    } catch {
      case t: Throwable => println(" Unknown message or file format " + ExceptionUtils.getStackTrace(t));
    }
    processedStr
  }

  def isValidMT103Message(message: com.prowidesoftware.swift.model.mt.AbstractMT) = {
    message != null && message.isType(103)
  }

  def getDirection(mt103: com.prowidesoftware.swift.model.mt.mt1xx.MT103) = {
    val direction = if (mt103.isOutput()) "Output" else "Input"
    direction
  }

  def parseMessageDetail(mt103: MT103, direction: String) = {
    concat(safeStr(mt103.getSender()),
      safeStr(mt103.getField53A().getBIC()),
      safeStr(mt103.getField53D().getNameAndAddress()),
      safeStr(mt103.getField59().getNameAndAddress()),
      safeStr(mt103.getField57A().getBIC()),
      safeStr(mt103.getField59F().getNameAndAddress1()),
      safeStr(mt103.getField32A().getAmount()),
      safeStr(mt103.getField32A().getCurrency()),
      safeStr(mt103.getField70().getComponent1()),
      direction)
  }

  def safeStr(str: Object) = {
    try {
      str.toString()
    } catch {
      case t: Throwable => " ";
    }
  }

  def concat(ss: String*) = ss filter (_.nonEmpty) mkString "\t "
}
