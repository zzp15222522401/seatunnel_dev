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

package org.apache.seatunnel.flink.clickhouse.sink.inject;

import ru.yandex.clickhouse.ClickHousePreparedStatementImpl;

import java.sql.SQLException;
import java.util.regex.Pattern;

public class StringInjectFunction implements ClickhouseFieldInjectFunction {

    private static final Pattern LOW_CARDINALITY_PATTERN = Pattern.compile("LowCardinality\\((.*)\\)");

    @Override
    public void injectFields(ClickHousePreparedStatementImpl statement, int index, Object value) throws SQLException {
        statement.setString(index, value.toString());
    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        return "String".equals(fieldType) || LOW_CARDINALITY_PATTERN.matcher(fieldType).matches();
    }
}
