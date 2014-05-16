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

import java.io.IOException;
import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.store.postgres.PostgresStoragePlugin;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class BasePostgresTest extends BaseTestQuery {

  // configurea
  // protected static Configuration conf = HBaseConfiguration.create();

  @Rule public TestName TEST_NAME = new TestName();

  private int columnWidth = 8;

  @Before
  public void printID() throws Exception {
    System.out.printf("Running %s#%s\n", getClass().getName(), TEST_NAME.getMethodName());
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    /*
     * Change the following to PostgresTestsSuite.configure(true, true) if you want
     * to test against a mini HBase cluster running within unit test environment
     */
    //PostgresTestsSuite.configure();

    PostgresTestsSuite.initCluster();
    PostgresStoragePlugin plugin = (PostgresStoragePlugin) bit.getContext().getStorage().getPlugin("postgres");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    PostgresTestsSuite.tearDownCluster();
  }

  protected String getPlanText(String planFile, String tableName) throws IOException {
    return Files.toString(FileUtils.getResourceAsFile(planFile), Charsets.UTF_8)
        .replaceFirst("\"hbase\\.zookeeper\\.property\\.clientPort\".*:.*\\d+", "\"hbase.zookeeper.property.clientPort\" : " + PostgresTestsSuite.getZookeeperPort())
        .replace("[TABLE_NAME]", tableName);
  }

  protected void runPhysicalVerifyCount(String planFile, String tableName, int expectedRowCount) throws Exception{
    String physicalPlan = getPlanText(planFile, tableName);
    List<QueryResultBatch> results = testPhysicalWithResults(physicalPlan);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  protected void runSQLVerifyCount(String sql, int expectedRowCount) throws Exception{
    sql = canonizeSQL(sql);
    System.out.println("Running query:\n" + sql);
    List<QueryResultBatch> results = testSqlWithResults(sql);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  private void printResultAndVerifyRowCount(List<QueryResultBatch> results, int expectedRowCount) throws SchemaChangeException {
    int rowCount = 0;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(QueryResultBatch result : results){
      rowCount += result.getHeader().getRowCount();
      loader.load(result.getHeader().getDef(), result.getData());
      if (loader.getRecordCount() <= 0) {
        break;
      }
      VectorUtil.showVectorAccessibleContent(loader, columnWidth);
      loader.clear();
      result.release();
    }
    System.out.println("Total record count: " + rowCount);
    if (expectedRowCount != -1) {
      Assert.assertEquals(expectedRowCount, rowCount);
    }
  }

  protected String canonizeSQL(String sql) {
    return sql.replace("[TABLE_NAME]", PostgresTestsSuite.TEST_TABLE_1);
  }

}
