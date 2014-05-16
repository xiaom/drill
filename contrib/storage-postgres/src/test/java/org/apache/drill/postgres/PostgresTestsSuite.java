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
package org.apache.drill.postgres;

import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Suite.class)
@SuiteClasses({
  PostgresRecordReaderPostgresTest.class,
  TestHBasePostgresFilterPushDown.class,
  TestHBasePostgresProjectPushDown.class})
public class PostgresTestsSuite {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PostgresTestsSuite.class);

  private static final boolean IS_DEBUG = ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  protected static final String TEST_TABLE_1 = "TestTable1";

  private static Configuration conf;


  private static volatile AtomicInteger initCount = new AtomicInteger(0);

  private static boolean createTables = System.getProperty("drill.hbase.tests.createTables", "true").equalsIgnoreCase("true");
  private static boolean tablesCreated = false;

  @BeforeClass
  public static void initCluster() throws Exception {
    /*
    if (initCount.get() == 0) {
      synchronized (PostgresTestsSuite.class) {
        if (initCount.get() == 0) {
          conf = HBaseConfiguration.create();
          if (IS_DEBUG) {
            conf.set("hbase.regionserver.lease.period","10000000");
          }

          if (manageHBaseCluster) {
            logger.info("Starting HBase mini cluster.");
            UTIL = new HBaseTestingUtility(conf);
            UTIL.startMiniCluster();
            hbaseClusterCreated = true;
            logger.info("HBase mini cluster started. Zookeeper port: '{}'", getZookeeperPort());
          }

          admin = new HBaseAdmin(conf);

          if (createTables || !tablesExist()) {
            createTestTables();
            tablesCreated = true;
          }
          initCount.incrementAndGet();
          return;
        }
      }
    }
    initCount.incrementAndGet();
    */
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
      /*
    synchronized (PostgresTestsSuite.class) {
      if (initCount.decrementAndGet() == 0) {
        if (createTables && tablesCreated) {
          cleanupTestTables();
        }

        if (admin != null) {
          admin.close();
        }

        if (hbaseClusterCreated) {
          logger.info("Shutting down HBase mini cluster.");
          UTIL.shutdownMiniCluster();
          logger.info("HBase mini cluster stopped.");
        }
      }
    }
      */
  }

  public static Configuration getConf() {
    return conf;
  }

  private static boolean tablesExist() throws IOException {
    //return admin.tableExists(TEST_TABLE_1);
    return true;
  }

  private static void createTestTables() throws Exception {
    /*
     * We are seeing some issues with (Drill) Filter operator if a group scan span
     * multiple fragments. Hence the number of regions in the HBase table is set to 1.
     * Will revert to multiple region once the issue is resolved.
     */
    //TestTableGenerator.generateHBaseDataset1(admin, TEST_TABLE_1, 1);
  }

  private static void cleanupTestTables() throws IOException {
    //admin.disableTable(TEST_TABLE_1);
    //admin.deleteTable(TEST_TABLE_1);
  }


  public static void configure() {
    // configuration for the unit test
    ;
  }

}
