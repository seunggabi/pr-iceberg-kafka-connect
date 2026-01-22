/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.IcebergWriter;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.WriterResult;
import io.tabular.iceberg.connect.events.EventTestUtil;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class WorkerTest {
  private static final String SRC_TOPIC_NAME = "src-topic";
  private static final String TABLE_NAME = "db.tbl";
  private static final String FIELD_NAME = "fld";

  @Test
  public void testStaticRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));
    when(config.catalogName()).thenReturn("catalog");
    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, "val");
    workerTest(config, value);
  }

  @Test
  public void testDynamicRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.catalogName()).thenReturn("catalog");

    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    workerTest(config, value);
  }

  @Test
  public void testDynamicRouteWithMixedCaseTableName() {
    // Test that table names with mixed case (uppercase letters) are preserved when case-sensitive is enabled
    String mixedCaseTableName = "db.MyTable_WithMixedCase";

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.catalogName()).thenReturn("catalog");
    when(config.dynamicTableNameCaseSensitive()).thenReturn(true);

    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, mixedCaseTableName);

    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(mixedCaseTableName),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Writer worker = new Worker(config, writerFactory);

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.write(ImmutableList.of(rec));

    Committable committable = worker.committable();

    assertThat(committable.offsetsByTopicPartition()).hasSize(1);
    assertThat(committable.writerResults()).hasSize(1);

    // Verify that the table name in the result preserves the original case
    WriterResult result = committable.writerResults().get(0);
    assertThat(result.tableIdentifier().toString()).isEqualTo(mixedCaseTableName);
  }

  @Test
  public void testDynamicRouteWithUpperCaseTableName() {
    // Test similar to the actual error case: btm_daily_cash_supplyPlan1_tb
    String tableName = "db.btm_daily_cash_supplyPlan1_tb";

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.catalogName()).thenReturn("catalog");
    when(config.dynamicTableNameCaseSensitive()).thenReturn(true);

    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, tableName);

    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(tableName),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Writer worker = new Worker(config, writerFactory);

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.write(ImmutableList.of(rec));

    Committable committable = worker.committable();

    assertThat(committable.offsetsByTopicPartition()).hasSize(1);
    assertThat(committable.writerResults()).hasSize(1);

    // Verify that the table name preserves the original case (not converted to lowercase)
    WriterResult result = committable.writerResults().get(0);
    assertThat(result.tableIdentifier().toString()).isEqualTo(tableName);
    // Ensure it's NOT converted to lowercase
    assertThat(result.tableIdentifier().toString()).isNotEqualTo(tableName.toLowerCase());
  }

  @Test
  public void testDynamicRouteWithMixedCaseTableNameBackwardsCompatibility() {
    // Test backwards compatibility: with case-sensitive disabled (default), table names are converted to lowercase
    String mixedCaseTableName = "db.MyTable_WithMixedCase";
    String expectedLowercaseTableName = mixedCaseTableName.toLowerCase();

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.catalogName()).thenReturn("catalog");
    when(config.dynamicTableNameCaseSensitive()).thenReturn(false); // Default behavior

    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, mixedCaseTableName);

    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(expectedLowercaseTableName),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Writer worker = new Worker(config, writerFactory);

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.write(ImmutableList.of(rec));

    Committable committable = worker.committable();

    assertThat(committable.offsetsByTopicPartition()).hasSize(1);
    assertThat(committable.writerResults()).hasSize(1);

    // Verify that the table name was converted to lowercase for backwards compatibility
    WriterResult result = committable.writerResults().get(0);
    assertThat(result.tableIdentifier().toString()).isEqualTo(expectedLowercaseTableName);
    assertThat(result.tableIdentifier().toString()).isNotEqualTo(mixedCaseTableName);
  }

  @Test
  public void testDynamicRouteTableNameCaseMismatch() {
    // Test that with case-sensitive=false, the writer is called with lowercase table name
    String mixedCaseTableName = "db.MyTable_WithMixedCase";
    String expectedLowercaseTableName = mixedCaseTableName.toLowerCase();

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.catalogName()).thenReturn("catalog");
    when(config.dynamicTableNameCaseSensitive()).thenReturn(false);

    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, mixedCaseTableName);

    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(expectedLowercaseTableName),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Writer worker = new Worker(config, writerFactory);

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.write(ImmutableList.of(rec));

    // Verify that createWriter was called with the lowercase table name
    verify(writerFactory).createWriter(eq(expectedLowercaseTableName), any(), anyBoolean());
  }

  @Test
  public void testDynamicRouteTableNameCaseSensitivePreservesCase() {
    // Test that with case-sensitive=true, the writer is called with the original case
    String mixedCaseTableName = "db.MyTable_WithMixedCase";

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.catalogName()).thenReturn("catalog");
    when(config.dynamicTableNameCaseSensitive()).thenReturn(true);

    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, mixedCaseTableName);

    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(mixedCaseTableName),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Writer worker = new Worker(config, writerFactory);

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.write(ImmutableList.of(rec));

    // Verify that createWriter was called with the original mixed-case table name (NOT lowercase)
    verify(writerFactory).createWriter(eq(mixedCaseTableName), any(), anyBoolean());
  }

  private void workerTest(IcebergSinkConfig config, Map<String, Object> value) {
    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Writer worker = new Worker(config, writerFactory);

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.write(ImmutableList.of(rec));

    Committable committable = worker.committable();

    assertThat(committable.offsetsByTopicPartition()).hasSize(1);
    // offset should be one more than the record offset
    assertThat(
            committable
                .offsetsByTopicPartition()
                .get(committable.offsetsByTopicPartition().keySet().iterator().next())
                .offset())
        .isEqualTo(1L);
  }
}
