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

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;

import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.hadoop.postgres.HTableDescriptor;
import org.apache.hadoop.postgres.client.PostgresAdmin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

public class PostgresSchemaFactory implements SchemaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PostgresSchemaFactory.class);

  final String schemaName;
  final PostgresStoragePlugin plugin;

  public PostgresSchemaFactory(PostgresStoragePlugin plugin, String name) throws IOException {
    this.plugin = plugin;
    this.schemaName = name;
  }

  @Override
  public void registerSchemas(UserSession session, SchemaPlus parent) {
    PostgresSchema schema = new PostgresSchema(schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class PostgresSchema extends AbstractSchema {

    public PostgresSchema(String name) {
      super(ImmutableList.<String>of(), name);
    }

    public void setHolder(SchemaPlus plusOfThis) {
    }

    @Override
    public Schema getSubSchema(String name) {
      return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return Collections.emptySet();
    }

    @Override
    public Table getTable(String name) {
      Object selection = new PostgresScanSpec(name);
      return new DynamicDrillTable(plugin, schemaName, selection);
    }

    @Override
    public Set<String> getTableNames() {
      try(PostgresAdmin admin = new PostgresAdmin(plugin.getConfig().getPostgresConf())) {
        HTableDescriptor[] tables = admin.listTables();
        Set<String> tableNames = Sets.newHashSet();
        for (HTableDescriptor table : tables) {
          tableNames.add(new String(table.getName()));
        }
        return tableNames;
      } catch (Exception e) {
        logger.warn("Failure while loading table names for database '{}'.", schemaName, e.getCause());
        return Collections.emptySet();
      }
    }
  }
}
