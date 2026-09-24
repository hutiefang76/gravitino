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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/** Encodes ClickHouse projection metadata in a table property and renders native DDL. */
final class ClickHouseProjectionMetadata {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern SETTING_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  private static final Pattern NUMERIC_VALUE =
      Pattern.compile("[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?");

  private ClickHouseProjectionMetadata() {}

  static ArrayNode newArray() {
    return MAPPER.createArrayNode();
  }

  static void add(
      ArrayNode projections, String name, String type, String query, String settingsJson) {
    ObjectNode projection = projections.addObject();
    projection.put("name", name);
    projection.put("type", type);
    projection.put("query", query);
    try {
      JsonNode settings = MAPPER.readTree(settingsJson);
      if (!settings.isObject()) {
        throw new IllegalArgumentException("Invalid ClickHouse projection settings metadata");
      }
      projection.set("settings", settings);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid ClickHouse projection settings metadata", e);
    }
  }

  static String encode(ArrayNode projections) {
    return projections.toString();
  }

  static String render(String property) {
    if (StringUtils.isBlank(property)) {
      return "";
    }
    JsonNode projections;
    try {
      projections = MAPPER.readTree(property);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid ClickHouse projections property", e);
    }
    if (!projections.isArray()) {
      throw new IllegalArgumentException("ClickHouse projections property must be a JSON array");
    }
    StringBuilder clauses = new StringBuilder();
    for (JsonNode projection : projections) {
      String name = requiredText(projection, "name");
      String type = requiredText(projection, "type");
      String query = requiredText(projection, "query");
      if (!"Normal".equals(type) && !"Aggregate".equals(type)) {
        throw new IllegalArgumentException("Unsupported ClickHouse projection type: " + type);
      }
      validateQuery(query);
      if (name.contains("`")) {
        throw new IllegalArgumentException("ClickHouse projection name cannot contain a backtick");
      }
      clauses.append(",\n  PROJECTION `").append(name).append("` (").append(query).append(")");
      JsonNode settings = projection.path("settings");
      if (!settings.isMissingNode() && !settings.isObject()) {
        throw new IllegalArgumentException("ClickHouse projection settings must be a JSON object");
      }
      if (settings.isObject() && !settings.isEmpty()) {
        clauses.append(" WITH SETTINGS (");
        Iterator<Map.Entry<String, JsonNode>> entries = settings.fields();
        while (entries.hasNext()) {
          Map.Entry<String, JsonNode> entry = entries.next();
          if (!SETTING_NAME.matcher(entry.getKey()).matches() || !entry.getValue().isTextual()) {
            throw new IllegalArgumentException("Invalid ClickHouse projection setting");
          }
          String value = entry.getValue().asText();
          clauses
              .append(entry.getKey())
              .append(" = ")
              .append(
                  NUMERIC_VALUE.matcher(value).matches()
                      ? value
                      : "'" + value.replace("'", "''") + "'");
          if (entries.hasNext()) {
            clauses.append(", ");
          }
        }
        clauses.append(")");
      }
    }
    return clauses.toString();
  }

  private static String requiredText(JsonNode projection, String field) {
    JsonNode value = projection.path(field);
    if (!value.isTextual() || StringUtils.isBlank(value.asText())) {
      throw new IllegalArgumentException("ClickHouse projection " + field + " must be a string");
    }
    return value.asText().trim();
  }

  private static void validateQuery(String query) {
    if (!query.matches("(?is)^SELECT\\b.*")) {
      throw new IllegalArgumentException(
          "ClickHouse projection query must be one SELECT statement");
    }
    char quote = 0;
    int depth = 0;
    for (int i = 0; i < query.length(); i++) {
      char current = query.charAt(i);
      char next = i + 1 < query.length() ? query.charAt(i + 1) : 0;
      if (quote != 0) {
        if (current == '\\' && next != 0) {
          i++;
        } else if (current == quote) {
          if (next == quote) {
            i++;
          } else {
            quote = 0;
          }
        }
      } else if (current == '\'' || current == '"' || current == '`') {
        quote = current;
      } else if (current == '(') {
        depth++;
      } else if (current == ')') {
        if (--depth < 0) {
          throw new IllegalArgumentException("Unbalanced ClickHouse projection query");
        }
      } else if (current == ';'
          || (current == '-' && next == '-')
          || (current == '/' && next == '*')) {
        throw new IllegalArgumentException(
            "ClickHouse projection query must be one SELECT statement");
      }
    }
    if (quote != 0 || depth != 0) {
      throw new IllegalArgumentException("Unbalanced ClickHouse projection query");
    }
  }
}
