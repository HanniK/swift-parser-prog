package com.scb.msg.processor

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import com.prowidesoftware.swift.model.mt.mt1xx.MT103
import com.prowidesoftware.swift.model.mt.AbstractMT
import java.io.File
import java.io._
import java.util.stream.Collectors
import org.apache.commons.lang.StringUtils

object ParseSwiftMT103 {

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

  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())
    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: MainExample <path-to-files> <output-path>")
      System.exit(1)
    }
    val jobName = "Parser-MT103"
    val conf = (new SparkConf().setMaster("local[1]").setAppName(jobName).set("spark.executor.memory", "1g"))
    val sc = new SparkContext(conf)
    val pathToFiles = arg(0)
    val outputPath = arg(1)
    val newLine = sys.props("line.separator")
    val files = sc.wholeTextFiles(pathToFiles)
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath)))
    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> pathToFiles \"" + pathToFiles + "\"")

    files.collect.foreach(
      tuple => {
        if (tuple._2.length() > 0) {
          var message: AbstractMT = null;
          try {
            message = AbstractMT.parse(tuple._2)
            if (isValidMT103Message(message)) {
              val mt103: MT103 = message.asInstanceOf[MT103]
              writer.write(parseMessageDetail(mt103, getDirection(mt103)))
              writer.write(newLine)
            }
          } catch { case t: Throwable => logger.error(" Unknown message or file format "); }
        }
      })
    writer.close()
  }
}
