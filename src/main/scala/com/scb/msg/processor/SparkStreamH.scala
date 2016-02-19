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
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object SparkStreamH {
  case class Record(key: Int, value: String)

  def main(args: Array[String]) {
    var logger = Logger.getLogger(this.getClass())
    val sparkConf = new SparkConf().setAppName("SparkStream")
    val ssc = new StreamingContext(sparkConf, Minutes(1))
    val dataStream = ssc.textFileStream("hdfs://xsgscbapp1a.scb.intra.theoptimum.net:9000/user/hive/warehouse/cers.db/swift/")
    val sc = ssc.sparkContext
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext.sql

    dataStream.foreachRDD { rdd =>
      rdd.filter(Swift.isMT103).map(Swift.convertSwift).toDF().registerTempTable("records")
      sql("INSERT INTO table cers.fine_grained_mt103 select r.* from records r ")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

