/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = new CachedConnectionProvider(this.dbDialect) {
      @Override
      protected void onConnect(Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records) throws SQLException {
    final Connection connection = cachedConnectionProvider.getConnection();

    final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
    for (SinkRecord record : records) {
      final TableId tableId = destinationTable(record);
      BufferedRecords buffer = bufferByTable.get(tableId);
      if (buffer == null) {
        buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
        bufferByTable.put(tableId, buffer);
      }
      buffer.add(record);
    }
    for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
      TableId tableId = entry.getKey();
      BufferedRecords buffer = entry.getValue();
      log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
      buffer.flush();
      buffer.close();
    }
    connection.commit();
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableNameFormatter newTableNameCustomFormatter(String className) {
    TableNameFormatter tableNameFormatter;
    final Class<?> tableNameCustomFormatterClass;
    try {
      tableNameCustomFormatterClass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new ConnectException(String.format(
          "Unable to find class: %s",
          className
      ));
    }

    if (!TableNameFormatter.class.isAssignableFrom(tableNameCustomFormatterClass)) {
      throw new ConnectException(String.format(
          "%s does not implement TableNameFormatter interface"
      ));
    }

    try {
      tableNameFormatter = (TableNameFormatter) tableNameCustomFormatterClass
          .getDeclaredConstructor().newInstance();
    } catch (IllegalAccessException | InstantiationException
        | NoSuchMethodException | InvocationTargetException e) {
      throw new ConnectException(String.format(
          "Unable to instantiate custom table formatter due to: %s",
          e.getMessage()
      ));
    }

    return tableNameFormatter;
  }

  TableId destinationTable(SinkRecord record) {
    final String tableName = config.tableNameCustomFormatter.isEmpty()
        ? config.tableNameFormat.replace("${topic}", record.topic()) :
        newTableNameCustomFormatter(config.tableNameCustomFormatter)
            .destinationTableName(record);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty",
          record.topic(),
          config.tableNameFormat
      ));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }
}
