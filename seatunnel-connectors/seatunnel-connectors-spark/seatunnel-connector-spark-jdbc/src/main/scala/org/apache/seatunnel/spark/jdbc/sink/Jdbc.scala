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
package org.apache.seatunnel.spark.jdbc.sink

import scala.collection.JavaConversions._
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.datasources.jdbc2.{JDBCSaveMode, JdbcOptionsInWrite, JdbcUtils}

class Jdbc extends SparkBatchSink {

  override def output(data: Dataset[Row], env: SparkEnvironment): Unit = {
    val saveMode = config.getString("saveMode")
    if (config.hasPath("definedsql")) {
      val definedsql = config.getString("definedsql")
      val configMap = Map(
          "driver" -> config.getString("driver"),
          "url" -> config.getString("url"),
          "user" -> config.getString("user"),
          "password" -> config.getString("password"),
          "dbtable" -> config.getString("dbTable")
      )
      JdbcUtils.executeSql(configMap, definedsql)
    }
    if ("update".equals(saveMode)) {
      data.write.format("org.apache.spark.sql.execution.datasources.jdbc2").options(
        Map(
          "saveMode" -> JDBCSaveMode.Update.toString,
          "driver" -> config.getString("driver"),
          "url" -> config.getString("url"),
          "user" -> config.getString("user"),
          "password" -> config.getString("password"),
          "dbtable" -> config.getString("dbTable"),
          "useSsl" -> config.getString("useSsl"),
          "customUpdateStmt" -> config.getString(
            "customUpdateStmt"
          ), // Custom mysql duplicate key update statement when saveMode is update
          "duplicateIncs" -> config.getString("duplicateIncs"),
          "showSql" -> config.getString("showSql"))).save()
    } else {
      val configMap = Map(
        "driver" -> config.getString("driver"),
        "url" -> config.getString("url"),
        "user" -> config.getString("user"),
        "password" -> config.getString("password"),
        "dbtable" -> config.getString("dbTable")
      )
      if (config.hasPath("createTableOptions")) {
        configMap.add("createTableOptions", config.getString("createTableOptions"))
      }
      if (config.hasPath("createTableColumnTypes")) {
        configMap.add("createTableColumnTypes", config.getString("createTableColumnTypes"))
      }
      if ("overwrite".equals(saveMode)) {
        val options = new JdbcOptionsInWrite(configMap)
        val conn = JdbcUtils.createConnectionFactory(options)()
        JdbcUtils.truncateTable(conn, options)
        data.write.format("jdbc").mode("append").options(configMap).save()
      }
      else {
        data.write.format("jdbc").mode(saveMode).options(configMap).save()

      }
    }

  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, "driver", "url", "dbTable", "user", "password")
  }

  override def prepare(prepareEnv: SparkEnvironment): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "saveMode" -> "error",
        "useSsl" -> "false",
        "showSql" -> "true",
        "customUpdateStmt" -> "",
        "duplicateIncs" -> ""))
    config = config.withFallback(defaultConfig)
  }

  override def getPluginName: String = "Jdbc"
}
