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

package org.apache.seatunnel.spark.file

object Config extends Serializable {

  final val PATH = "path"
  final val PARTITION_BY = "partition_by"
  final val SAVE_MODE = "save_mode"
  final val SERIALIZER = "serializer"
  final val PATH_TIME_FORMAT = "path_time_format"
  final val DEFAULT_TIME_FORMAT = "yyyyMMddHHmmss"
  final val FORMAT = "format"
  final val SAVE_MODE_ERROR = "error"
  final val OPTION_PREFIX = "options."
  final val FILE_COUNT = "file_count"

  final val TEXT = "text"
  final val PARQUET = "parquet"
  final val JSON = "json"
  final val ORC = "orc"
  final val CSV = "csv"

  final val DEFAULT_FORMAT = JSON

}
