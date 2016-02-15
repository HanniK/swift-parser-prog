/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.scb.msg.processor
import com.google.common.io.{ ByteStreams, Files }
import java.io.File
import com.prowidesoftware.swift.model.mt.mt1xx.MT103
import com.prowidesoftware.swift.model.mt.AbstractMT
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.Logger
import org.apache.commons.lang.exception.ExceptionUtils

object HiveFromSpark {
  case class Record(key: Int, value: String)

  def main(args: Array[String]) {
    var logger = Logger.getLogger(this.getClass())
    val sparkConf = new SparkConf().setAppName("HiveFromSpark")
    val sc = new SparkContext(sparkConf)

    // A hive context adds support for finding tables in the MetaStore and writing queries
    // using HiveQL. Users who do not have an existing Hive deployment can still create a
    // HiveContext. When not configured by the hive-site.xml, the context automatically
    // creates metastore_db and warehouse in the current directory.
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext.sql

    val dfSwift = sql("SELECT * FROM cers.swift").toDF()

    sql("USE cers")
    sql("CREATE TABLE IF NOT EXISTS fine_grain_swift (data String)")

    dfSwift.collect().foreach { x =>
      var message: AbstractMT = null;
      try {
        message = AbstractMT.parse(x.getString(0).replaceAll(" ", "\n"))
        if (isValidMT103Message(message)) {
          val mt103: MT103 = message.asInstanceOf[MT103]
          val processedStr = parseMessageDetail(mt103, getDirection(mt103))
          logger.info("Processed string is: " + processedStr)
          sc.parallelize(List(processedStr)).toDF().registerTempTable("records")
          sql("INSERT INTO table cers.fine_grain_swift select r.* from records r ")
        }
      } catch { case t: Throwable => logger.error(" Unknown message or file format " + ExceptionUtils.getStackTrace(t)); }
    }

    val rdd = sql("SELECT * FROM cers.fine_grain_swift")
    rdd.toDF().collect().foreach(println)

    sc.stop()
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
// scalastyle:on println

