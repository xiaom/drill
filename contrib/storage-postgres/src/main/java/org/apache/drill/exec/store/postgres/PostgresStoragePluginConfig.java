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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

@JsonTypeName("postgres")
public class PostgresStoragePluginConfig extends StoragePluginConfigBase implements DrillPostgresConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PostgresStoragePluginConfig.class);

  private Map<String, String> config;

  @JsonIgnore
  private Configuration postgresConf;

  @JsonCreator
  public PostgresStoragePluginConfig(@JsonProperty("config") Map<String, String> props) {
    this.config = props;
    if (config == null) {
      config = Maps.newHashMap();
    }
    /*
    logger.debug("Configuring Postgres StoragePlugin with zookeeper quorum '{}', port '{}'.",
        config.get(HConstants.ZOOKEEPER_QUORUM), config.get(HBASE_ZOOKEEPER_PORT));
    */
  }

  @JsonProperty
  public Map<String, String> getConfig() {
    return ImmutableMap.copyOf(config);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PostgresStoragePluginConfig that = (PostgresStoragePluginConfig) o;
    return config.equals(that.config);
  }

  @Override
  public int hashCode() {
    return this.config != null ? this.config.hashCode() : 0;
  }

  @JsonIgnore
  public Configuration getPostgresConf() {
    if (postgresConf == null) {
      // TODO: get the configuration of Postgres
      // postgresConf = PostgresConfiguration.create();

      if (config != null) {
        for (Map.Entry<String, String> entry : config.entrySet()) {
          // postgresConf.set(entry.getKey(), entry.getValue());
          // TODO: construct the conf object
        }
      }

    }

    // return postgresConf;
    return NULL;
  }
/*
  @JsonIgnore
  @VisibleForTesting
  public void setZookeeperPort(int zookeeperPort) {
    this.config.put(HBASE_ZOOKEEPER_PORT, String.valueOf(zookeeperPort));
    getPostgresConf().setInt(HBASE_ZOOKEEPER_PORT, zookeeperPort);
  }
*/

}
