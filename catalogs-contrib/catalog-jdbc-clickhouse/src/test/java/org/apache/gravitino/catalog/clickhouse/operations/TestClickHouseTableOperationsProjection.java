/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.catalog.clickhouse.operations;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseUtils.getSortOrders;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** Tests ClickHouse projection definitions in generated table DDL. */
public class TestClickHouseTableOperationsProjection {
  private final ClickHouseTableOperations operations = new ClickHouseTableOperations();

  @Test
  public void testCreateTableRendersMultipleProjectionDefinitions() {
    operations.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        Map.of());
    Map<String, String> properties =
        Map.of(
            "projections",
            """
            [{"name":"by-event","type":"Normal","query":"SELECT `event-date` ORDER BY `event-date`","settings":{}},
             {"name":"totals","type":"Aggregate","query":"SELECT `event-date`, count() GROUP BY `event-date`","settings":{}}]
            """);
    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("id")
              .withType(Types.LongType.get())
              .withNullable(false)
              .build(),
          JdbcColumn.builder()
              .withName("event-date")
              .withType(Types.DateType.get())
              .withNullable(false)
              .build()
        };

    String sql =
        operations.generateCreateTableSql(
            "events",
            columns,
            null,
            properties,
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new Index[0],
            getSortOrders("id"));

    Assertions.assertTrue(
        sql.contains("PROJECTION `by-event` (SELECT `event-date` ORDER BY `event-date`)"), sql);
    Assertions.assertTrue(
        sql.contains("PROJECTION `totals` (SELECT `event-date`, count() GROUP BY `event-date`)"),
        sql);
    Assertions.assertTrue(sql.indexOf("PROJECTION `totals`") < sql.indexOf("ENGINE ="), sql);
  }

  @Test
  public void testLoadProjectionDefinitionsWithSettings() throws Exception {
    Connection connection = mock(Connection.class);
    PreparedStatement columnsStatement = mock(PreparedStatement.class);
    PreparedStatement projectionsStatement = mock(PreparedStatement.class);
    ResultSet columns = mock(ResultSet.class);
    ResultSet projections = mock(ResultSet.class);
    when(connection.prepareStatement(anyString()))
        .thenReturn(columnsStatement, projectionsStatement);
    when(columnsStatement.executeQuery()).thenReturn(columns);
    when(columns.next()).thenReturn(true, true, false);
    when(columns.getString("name")).thenReturn("name", "settings");
    when(projectionsStatement.executeQuery()).thenReturn(projections);
    when(projections.next()).thenReturn(true, true, false);
    when(projections.getString("name")).thenReturn("by_event", "totals");
    when(projections.getString("type")).thenReturn("Normal", "Aggregate");
    when(projections.getString("query"))
        .thenReturn("SELECT event_date ORDER BY event_date", "SELECT count() GROUP BY event_date");
    when(projections.getString("settings_json"))
        .thenReturn("{}", "{\"index_granularity\":\"128\"}");

    String property = operations.getProjectionProperty(connection, "db", "events");

    Assertions.assertTrue(property.contains("\"name\":\"by_event\""), property);
    Assertions.assertTrue(property.contains("\"type\":\"Aggregate\""), property);
    Assertions.assertTrue(property.contains("\"index_granularity\":\"128\""), property);
    verify(projectionsStatement).setString(1, "db");
    verify(projectionsStatement).setString(2, "events");
  }

  @Test
  public void testOldServerWithoutProjectionTableSkipsMetadataQuery() throws Exception {
    Connection connection = mock(Connection.class);
    PreparedStatement columnsStatement = mock(PreparedStatement.class);
    ResultSet columns = mock(ResultSet.class);
    when(connection.prepareStatement(anyString())).thenReturn(columnsStatement);
    when(columnsStatement.executeQuery()).thenReturn(columns);
    when(columns.next()).thenReturn(false);

    Assertions.assertEquals("", operations.getProjectionProperty(connection, "db", "events"));
    verify(connection).prepareStatement(anyString());
  }

  @Test
  public void testServerWithoutProjectionSettingsUsesLegacyQuery() throws Exception {
    Connection connection = mock(Connection.class);
    PreparedStatement columnsStatement = mock(PreparedStatement.class);
    PreparedStatement projectionsStatement = mock(PreparedStatement.class);
    ResultSet columns = mock(ResultSet.class);
    ResultSet projections = mock(ResultSet.class);
    when(connection.prepareStatement(anyString()))
        .thenReturn(columnsStatement, projectionsStatement);
    when(columnsStatement.executeQuery()).thenReturn(columns);
    when(columns.next()).thenReturn(true, false);
    when(columns.getString("name")).thenReturn("name");
    when(projectionsStatement.executeQuery()).thenReturn(projections);
    when(projections.next()).thenReturn(true, false);
    when(projections.getString("name")).thenReturn("by_event");
    when(projections.getString("type")).thenReturn("Normal");
    when(projections.getString("query")).thenReturn("SELECT event_date ORDER BY event_date");

    String property = operations.getProjectionProperty(connection, "db", "events");

    Assertions.assertTrue(property.contains("\"settings\":{}"), property);
    verify(connection)
        .prepareStatement(
            "SELECT name, type, query FROM system.projections "
                + "WHERE database = ? AND table = ? ORDER BY name");
  }

  @Test
  public void testProjectionSettingsAndQuotedLiteralRemainInProjectionScope() {
    String clauses =
        ClickHouseProjectionMetadata.render(
            """
            [{"name":"by_day","type":"Normal",
              "query":"SELECT toDate('2026-09-24') AS day, id ORDER BY day, id",
              "settings":{"index_granularity":"128"}}]
            """);

    Assertions.assertEquals(
        ",\n  PROJECTION `by_day` (SELECT toDate('2026-09-24') AS day, id ORDER BY day, id)"
            + " WITH SETTINGS (index_granularity = 128)",
        clauses);
  }

  @Test
  public void testRejectsProjectionQueryThatEscapesDefinition() {
    String injected =
        """
        [{"name":"bad","type":"Normal","query":"SELECT id) ENGINE = Memory --"}]
        """;

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ClickHouseProjectionMetadata.render(injected));
  }

  @Test
  public void testMalformedProjectionPropertyIsRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseProjectionMetadata.render("{\"name\":\"not-an-array\"}"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ClickHouseProjectionMetadata.render(
                "[{\"name\":\"p\",\"type\":\"Normal\",\"query\":\"DELETE FROM t\"}]"));
  }

  @Test
  public void testProjectionDefinitionsRequireMergeTreeColumns() {
    operations.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        Map.of());
    String projection =
        "[{\"name\":\"p\",\"type\":\"Normal\",\"query\":\"SELECT id ORDER BY id\"}]";
    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("id")
              .withType(Types.LongType.get())
              .withNullable(false)
              .build()
        };

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            operations.generateCreateTableSql(
                "memory_table",
                columns,
                null,
                Map.of("engine", "Memory", "projections", projection),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new Index[0],
                new SortOrder[0]));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            operations.generateCreateTableSql(
                "empty_table",
                new JdbcColumn[0],
                null,
                Map.of("projections", projection),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                new Index[0],
                getSortOrders("id")));
  }

  @Test
  public void testNativeProjectionRoundTripAgainstClickHouse24_9() throws Exception {
    String url = System.getenv("CLICKHOUSE_PROJECTION_TEST_URL");
    Assumptions.assumeTrue(url != null, "Set CLICKHOUSE_PROJECTION_TEST_URL for ClickHouse 24.9+");
    String database = "projection_roundtrip_" + UUID.randomUUID().toString().replace("-", "");
    String source = "events_source";
    String recreated = "events_recreated";
    try (Connection connection = DriverManager.getConnection(url, "default", "default");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS " + database);
      try {
        statement.execute(
            "CREATE TABLE "
                + database
                + "."
                + source
                + " (id Int64, event_date Date, amount Int64,"
                + " PROJECTION `by-event` (SELECT id, event_date ORDER BY event_date, id))"
                + " ENGINE = MergeTree ORDER BY id");
        statement.execute(
            "ALTER TABLE "
                + database
                + "."
                + source
                + " ADD PROJECTION totals"
                + " (SELECT event_date, sum(amount) GROUP BY event_date)");

        ClickHouseTableOperations catalog = new ClickHouseTableOperations();
        catalog.initialize(
            DataSourceUtils.createDataSource(
                Map.of(
                    JdbcConfig.JDBC_URL.getKey(), url + "/" + database,
                    JdbcConfig.JDBC_DRIVER.getKey(), "com.clickhouse.jdbc.ClickHouseDriver",
                    JdbcConfig.USERNAME.getKey(), "default",
                    JdbcConfig.PASSWORD.getKey(), "default")),
            new ClickHouseExceptionConverter(),
            new ClickHouseTypeConverter(),
            new ClickHouseColumnDefaultValueConverter(),
            Map.of());
        JdbcTable loaded = catalog.load(database, source);
        Assertions.assertTrue(loaded.properties().get("projections").contains("by-event"));
        Assertions.assertTrue(loaded.properties().get("projections").contains("totals"));
        catalog.create(
            database,
            recreated,
            Arrays.stream(loaded.columns())
                .map(column -> (JdbcColumn) column)
                .toArray(JdbcColumn[]::new),
            loaded.comment(),
            loaded.properties(),
            loaded.partitioning(),
            loaded.distribution(),
            loaded.index(),
            loaded.sortOrder());
        Assertions.assertEquals(
            projectionDefinitions(connection, database, source),
            projectionDefinitions(connection, database, recreated));
      } finally {
        statement.execute("DROP TABLE IF EXISTS " + database + "." + recreated);
        statement.execute("DROP TABLE IF EXISTS " + database + "." + source);
        statement.execute("DROP DATABASE IF EXISTS " + database);
      }
    }
  }

  private Map<String, String> projectionDefinitions(
      Connection connection, String database, String table) throws SQLException {
    Map<String, String> projections = new LinkedHashMap<>();
    try (PreparedStatement statement =
        connection.prepareStatement(
            "SELECT name, type, query FROM system.projections "
                + "WHERE database = ? AND table = ? ORDER BY name")) {
      statement.setString(1, database);
      statement.setString(2, table);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          projections.put(
              resultSet.getString("name"),
              resultSet.getString("type") + ":" + resultSet.getString("query"));
        }
      }
    }
    return projections;
  }
}
