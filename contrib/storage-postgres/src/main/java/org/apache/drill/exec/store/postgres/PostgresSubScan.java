/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.postgres;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

// Class containing information for reading a single Postgres region
@JsonTypeName("postgres-region-scan")
public class PostgresSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PostgresSubScan.class);

  @JsonProperty
  public final PostgresStoragePluginConfig storage;
  @JsonIgnore
  private final PostgresStoragePlugin postgresStoragePlugin;
  private final List<PostgresSubScanSpec> regionScanSpecList;
  private final List<SchemaPath> columns;

  @JsonCreator
  public PostgresSubScan(@JacksonInject StoragePluginRegistry registry,
                         @JsonProperty("storage") StoragePluginConfig storage,
                         @JsonProperty("regionScanSpecList") LinkedList<PostgresSubScanSpec> regionScanSpecList,
                         @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    postgresStoragePlugin = (PostgresStoragePlugin) registry.getPlugin(storage);
    this.regionScanSpecList = regionScanSpecList;
    this.storage = (PostgresStoragePluginConfig) storage;
    this.columns = columns;
  }

  public PostgresSubScan(PostgresStoragePlugin plugin, PostgresStoragePluginConfig config,
                         List<PostgresSubScanSpec> regionInfoList, List<SchemaPath> columns) {
    postgresStoragePlugin = plugin;
    storage = config;
    this.regionScanSpecList = regionInfoList;
    this.columns = columns;
  }

  public List<PostgresSubScanSpec> getRegionScanSpecList() {
    return regionScanSpecList;
  }

  @JsonIgnore
  public PostgresStoragePluginConfig getStorageConfig() {
    return storage;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public OperatorCost getCost() {
    return null;
  }

  @Override
  public Size getSize() {
    return null;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public PostgresStoragePlugin getStorageEngine(){
    return postgresStoragePlugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new PostgresSubScan(postgresStoragePlugin, storage, regionScanSpecList, columns);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class PostgresSubScanSpec {

    protected String tableName;
    protected byte[] startRow;
    protected byte[] stopRow;
    protected byte[] serializedFilter;

    @parquet.org.codehaus.jackson.annotate.JsonCreator
    public PostgresSubScanSpec(@JsonProperty("tableName") String tableName,
                            @JsonProperty("startRow") byte[] startRow,
                            @JsonProperty("stopRow") byte[] stopRow,
                            @JsonProperty("serializedFilter") byte[] serializedFilter,
                            @JsonProperty("filterString") String filterString) {
      if (serializedFilter != null && filterString != null) {
        throw new IllegalArgumentException("The parameters 'serializedFilter' or 'filterString' cannot be specified at the same time.");
      }
      this.tableName = tableName;
      this.startRow = startRow;
      this.stopRow = stopRow;
      if (serializedFilter != null) {
        this.serializedFilter = serializedFilter;
      } else {
        this.serializedFilter = PostgresUtils.serializeFilter(PostgresUtils.parseFilterString(filterString));
      }
    }

    /* package */ PostgresSubScanSpec() {
      // empty constructor, to be used with builder pattern;
    }

    @JsonIgnore
    private Filter scanFilter;
    public Filter getScanFilter() {
      if (scanFilter == null &&  serializedFilter != null) {
          scanFilter = PostgresUtils.deserializeFilter(serializedFilter);
      }
      return scanFilter;
    }

    public String getTableName() {
      return tableName;
    }

    public PostgresSubScanSpec setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public byte[] getStartRow() {
      return startRow;
    }

    public PostgresSubScanSpec setStartRow(byte[] startRow) {
      this.startRow = startRow;
      return this;
    }

    public byte[] getStopRow() {
      return stopRow;
    }

    public PostgresSubScanSpec setStopRow(byte[] stopRow) {
      this.stopRow = stopRow;
      return this;
    }

    public byte[] getSerializedFilter() {
      return serializedFilter;
    }

    public PostgresSubScanSpec setSerializedFilter(byte[] serializedFilter) {
      this.serializedFilter = serializedFilter;
      this.scanFilter = null;
      return this;
    }

    @Override
    public String toString() {
      return "PostgresScanSpec [tableName=" + tableName
          + ", startRow=" + (startRow == null ? null : Bytes.toStringBinary(startRow))
          + ", stopRow=" + (stopRow == null ? null : Bytes.toStringBinary(stopRow))
          + ", filter=" + (getScanFilter() == null ? null : getScanFilter().toString())
          + "]";
    }

  }

}
