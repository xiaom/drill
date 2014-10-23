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

import java.util.Arrays;

import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.hadoop.postgres.HConstants;
import org.apache.hadoop.postgres.filter.BinaryComparator;
import org.apache.hadoop.postgres.filter.CompareFilter.CompareOp;
import org.apache.hadoop.postgres.filter.Filter;
import org.apache.hadoop.postgres.filter.NullComparator;
import org.apache.hadoop.postgres.filter.RowFilter;
import org.apache.hadoop.postgres.filter.SingleColumnValueFilter;
import org.apache.hadoop.postgres.filter.WritableByteArrayComparable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class PostgresFilterBuilder extends AbstractExprVisitor<PostgresScanSpec, Void, RuntimeException> implements DrillPostgresConstants {

  final private PostgresGroupScan groupScan;

  final private LogicalExpression le;

  private boolean allExpressionsConverted = true;

  PostgresFilterBuilder(PostgresGroupScan groupScan, LogicalExpression le) {
    this.groupScan = groupScan;
    this.le = le;
  }

  public PostgresScanSpec parseTree() {
    return mergeScanSpecs("booleanAnd", this.groupScan.getPostgresScanSpec(), le.accept(this, null));
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public PostgresScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public PostgresScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    PostgresScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;
    if (COMPARE_FUNCTIONS_TRANSPOSE_MAP.containsKey(functionName)) {
      LogicalExpression nameArg = args.get(0);
      LogicalExpression valueArg = args.get(1);
      if (nameArg instanceof QuotedString) {
        valueArg = nameArg;
        nameArg = args.get(1);
        functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName);
      }

      while (nameArg instanceof CastExpression
          && nameArg.getMajorType().getMinorType() == MinorType.VARCHAR) {
        nameArg = ((CastExpression) nameArg).getInput();
      }

      if (nameArg instanceof SchemaPath && valueArg instanceof QuotedString) {
        nodeScanSpec = createPostgresScanSpec(functionName, (SchemaPath) nameArg, ((QuotedString) valueArg).value.getBytes());
      }
    } else {
      switch (functionName) {
      case "booleanAnd":
      case "booleanOr":
        PostgresScanSpec leftScanSpec = args.get(0).accept(this, null);
        PostgresScanSpec rightScanSpec = args.get(1).accept(this, null);
        if (leftScanSpec != null && rightScanSpec != null) {
          nodeScanSpec = mergeScanSpecs(functionName, leftScanSpec, rightScanSpec);
        } else {
          allExpressionsConverted = false;
          if ("booleanAnd".equals(functionName)) {
            nodeScanSpec = leftScanSpec == null ? rightScanSpec : leftScanSpec;
          }
        }
        break;
      case "isnotnull":
      case "isNotNull":
      case "is not null":
      case "isnull":
      case "isNull":
      case "is null":
        /*
         * HBASE-10848: Bug in Postgres versions (0.94.[0-18], 0.96.[0-2], 0.98.[0-1])
         * causes a filter with NullComparator to fail. Enable only if specified in
         * the configuration (after ensuring that the Postgres cluster has the fix).
         */
        if (groupScan.getPostgresConf().getBoolean("drill.postgres.supports.null.comparator", false)) {
          if (args.get(0) instanceof SchemaPath) {
            nodeScanSpec = createPostgresScanSpec(functionName, ((SchemaPath) args.get(0)), null);
          }
        }
      }
    }

    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }
    return nodeScanSpec;
  }

  private PostgresScanSpec mergeScanSpecs(String functionName, PostgresScanSpec leftScanSpec, PostgresScanSpec rightScanSpec) {
    Filter newFilter = null;
    byte[] startRow = HConstants.EMPTY_START_ROW;
    byte[] stopRow = HConstants.EMPTY_END_ROW;

    switch (functionName) {
    case "booleanAnd":
      newFilter = PostgresUtils.andFilterAtIndex(leftScanSpec.filter, PostgresUtils.LAST_FILTER, rightScanSpec.filter);
      startRow = PostgresUtils.maxOfStartRows(leftScanSpec.startRow, rightScanSpec.startRow);
      stopRow = PostgresUtils.minOfStopRows(leftScanSpec.stopRow, rightScanSpec.stopRow);
      break;
    case "booleanOr":
      newFilter = PostgresUtils.orFilterAtIndex(leftScanSpec.filter, PostgresUtils.LAST_FILTER, rightScanSpec.filter);
      startRow = PostgresUtils.minOfStartRows(leftScanSpec.startRow, rightScanSpec.startRow);
      stopRow = PostgresUtils.maxOfStopRows(leftScanSpec.stopRow, rightScanSpec.stopRow);
    }
    return new PostgresScanSpec(groupScan.getTableName(), startRow, stopRow, newFilter);
  }

  private PostgresScanSpec createPostgresScanSpec(String functionName, SchemaPath field, byte[] fieldValue) {
    boolean isRowKey = field.getAsUnescapedPath().equals(ROW_KEY);
    if (!(isRowKey
        || (field.getRootSegment().getChild() != null && field.getRootSegment().getChild().isNamed()))) {
      /*
       * if the field in this function is neither the row_key nor a qualified Postgres column, return.
       */
      return null;
    }

    CompareOp compareOp = null;
    boolean isNullTest = false;
    WritableByteArrayComparable comparator = new BinaryComparator(fieldValue);
    byte[] startRow = HConstants.EMPTY_START_ROW;
    byte[] stopRow = HConstants.EMPTY_END_ROW;
    switch (functionName) {
    case "equal":
      compareOp = CompareOp.EQUAL;
      if (isRowKey) {
        startRow = stopRow = fieldValue;
      }
      break;
    case "not_equal":
      compareOp = CompareOp.NOT_EQUAL;
      break;
    case "greater_than_or_equal_to":
      compareOp = CompareOp.GREATER_OR_EQUAL;
      if (isRowKey) {
        startRow = fieldValue;
      }
      break;
    case "greater_than":
      compareOp = CompareOp.GREATER;
      if (isRowKey) {
        startRow = fieldValue;
      }
      break;
    case "less_than_or_equal_to":
      compareOp = CompareOp.LESS_OR_EQUAL;
      if (isRowKey) {
        // stopRow should be just greater than 'value'
        stopRow = Arrays.copyOf(fieldValue, fieldValue.length+1);
      }
      break;
    case "less_than":
      compareOp = CompareOp.LESS;
      if (isRowKey) {
        stopRow = fieldValue;
      }
      break;
    case "isnull":
    case "isNull":
    case "is null":
      if (isRowKey) {
        return null;
      }
      isNullTest = true;
      compareOp = CompareOp.EQUAL;
      comparator = new NullComparator();
      break;
    case "isnotnull":
    case "isNotNull":
    case "is not null":
      if (isRowKey) {
        return null;
      }
      compareOp = CompareOp.NOT_EQUAL;
      comparator = new NullComparator();
      break;
    }

    if (compareOp != null || startRow != HConstants.EMPTY_START_ROW || stopRow != HConstants.EMPTY_END_ROW) {
      Filter filter = null;
      if (isRowKey) {
        if (compareOp != null) {
          filter = new RowFilter(compareOp, comparator);
        }
      } else {
        byte[] family = PostgresUtils.getBytes(field.getRootSegment().getPath());
        byte[] qualifier = PostgresUtils.getBytes(field.getRootSegment().getChild().getNameSegment().getPath());
        filter = new SingleColumnValueFilter(family, qualifier, compareOp, comparator);
        ((SingleColumnValueFilter)filter).setLatestVersionOnly(true);
        if (!isNullTest) {
          ((SingleColumnValueFilter)filter).setFilterIfMissing(true);
        }
      }
      return new PostgresScanSpec(groupScan.getTableName(), startRow, stopRow, filter);
    }
    // else
    return null;
  }

  private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;
  static {
   Builder<String, String> builder = ImmutableMap.builder();
   COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
       .put("equal", "equal")
       .put("not_equal", "not_equal")
       .put("greater_than_or_equal_to", "less_than_or_equal_to")
       .put("greater_than", "less_than")
       .put("less_than_or_equal_to", "greater_than_or_equal_to")
       .put("less_than", "greater_than")
       .build();
  }

}
