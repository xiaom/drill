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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import net.hydromatic.optiq.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

import java.io.IOException;
import java.util.Set;

public class PostgresStoragePlugin extends AbstractStoragePlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PostgresStoragePlugin.class);

  private final DrillbitContext context;
  private final org.apache.drill.exec.store.postgres.PostgresStoragePluginConfig engineConfig;
  private final PostgresSchemaFactory schemaFactory;

  @SuppressWarnings("unused")
  private final String name;

  public PostgresStoragePlugin(PostgresStoragePluginConfig configuration, DrillbitContext context, String name)
      throws IOException {
    this.context = context;
    this.schemaFactory = new PostgresSchemaFactory(this, name);
    this.engineConfig = configuration;
    this.name = name;
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public PostgresGroupScan getPhysicalScan(JSONOptions selection) throws IOException {
    PostgresScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<PostgresScanSpec>() {});
    return new PostgresGroupScan(this, scanSpec, null);
  }

  @Override
  public void registerSchemas(UserSession session, SchemaPlus parent) {
    schemaFactory.registerSchemas(session, parent);
  }

  @Override
  public PostgresStoragePluginConfig getConfig() {
    return engineConfig;
  }

  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of(PostgresPushFilterIntoScan.INSTANCE);
  }

}