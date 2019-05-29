/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.side;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.HashBasedTable;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.table.api.Table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.SqlKind.*;

/**
 * Reason:
 * Date: 2018/7/24
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class SideSqlExec
        implements Serializable
{

    public AliasInfo parseASNode(SqlNode sqlNode)
            throws SqlParseException
    {
        SqlKind sqlKind = sqlNode.getKind();
        if (sqlKind != AS) {
            throw new RuntimeException(sqlNode + " is not 'as' operator");
        }

        SqlNode info = ((SqlBasicCall) sqlNode).getOperands()[0];
        SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];

        AliasInfo aliasInfo = new AliasInfo();
        aliasInfo.setName(info.toString());
        aliasInfo.setAlias(alias.toString());

        return aliasInfo;
    }

    public RowTypeInfo buildOutRowTypeInfo(List<FieldInfo> sideJoinFieldInfo, HashBasedTable<String, String, String> mappingTable)
    {
        TypeInformation[] sideOutTypes = new TypeInformation[sideJoinFieldInfo.size()];
        String[] sideOutNames = new String[sideJoinFieldInfo.size()];
        for (int i = 0; i < sideJoinFieldInfo.size(); i++) {
            FieldInfo fieldInfo = sideJoinFieldInfo.get(i);
            String tableName = fieldInfo.getTable();
            String fieldName, mappingFieldName;

            if (fieldInfo.getRawFieldName() != null) {
                fieldName = fieldInfo.getFieldName();
                mappingFieldName = fieldInfo.getFieldName();
                // TODO 输出字段是源字段？还是大写字段
                // mappingFieldName = fieldInfo.getRawFieldName();
            }
            else {
                fieldName = fieldInfo.getFieldName().toUpperCase();
                mappingFieldName = fieldInfo.getFieldName();
            }

            if (!mappingTable.column(fieldName).isEmpty()) {
                mappingFieldName = tableName + "." + mappingFieldName;
            }

            mappingTable.put(tableName, fieldName, mappingFieldName);

            sideOutTypes[i] = fieldInfo.getTypeInformation();
            sideOutNames[i] = mappingFieldName;
        }

        return new RowTypeInfo(sideOutTypes, sideOutNames);
    }

    //需要考虑更多的情况
    public void replaceFieldName(SqlNode sqlNode, HashBasedTable<String, String, String> mappingTable, String targetTableName, String tableAlias)
    {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case INSERT:
                SqlNode sqlSource = ((SqlInsert) sqlNode).getSource();
                replaceFieldName(sqlSource, mappingTable, targetTableName, tableAlias);
                break;
            case AS:
                SqlNode asNode = ((SqlBasicCall) sqlNode).getOperands()[0];
                replaceFieldName(asNode, mappingTable, targetTableName, tableAlias);
                break;
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) filterNodeWithTargetName(sqlNode, targetTableName);
                if (sqlSelect == null) {
                    return;
                }

                SqlNode sqlSource1 = sqlSelect.getFrom();
                if (sqlSource1.getKind() == AS) {
                    String tableName = ((SqlBasicCall) sqlSource1).getOperands()[0].toString();
                    if (tableName.equalsIgnoreCase(targetTableName)) {
                        SqlNodeList sqlSelectList = sqlSelect.getSelectList();
                        SqlNode whereNode = sqlSelect.getWhere();
                        SqlNodeList sqlGroup = sqlSelect.getGroup();

                        //TODO 暂时不处理having
                        SqlNode sqlHaving = sqlSelect.getHaving();

                        List<SqlNode> newSelectNodeList = Lists.newArrayList();
                        for (int i = 0; i < sqlSelectList.getList().size(); i++) {
                            SqlNode selectNode = sqlSelectList.getList().get(i);
                            //特殊处理 isStar的标识
                            if (selectNode.getKind() == IDENTIFIER && ((SqlIdentifier) selectNode).isStar()) {
                                List<SqlNode> replaceNodeList = replaceSelectStarFieldName(selectNode, mappingTable, tableAlias);
                                newSelectNodeList.addAll(replaceNodeList);
                                continue;
                            }

                            SqlNode replaceNode = replaceSelectFieldName(selectNode, mappingTable, tableAlias);
                            if (replaceNode == null) {
                                continue;
                            }

                            //sqlSelectList.set(i, replaceNode);
                            newSelectNodeList.add(replaceNode);
                        }

                        SqlNodeList newSelectList = new SqlNodeList(newSelectNodeList, sqlSelectList.getParserPosition());
                        sqlSelect.setSelectList(newSelectList);

                        //where
                        if (whereNode != null) {
                            SqlNode[] sqlNodeList = ((SqlBasicCall) whereNode).getOperands();
                            for (int i = 0; i < sqlNodeList.length; i++) {
                                SqlNode whereSqlNode = sqlNodeList[i];
                                SqlNode replaceNode = replaceNodeInfo(whereSqlNode, mappingTable, tableAlias);
                                sqlNodeList[i] = replaceNode;
                            }
                        }

                        if (sqlGroup != null && CollectionUtils.isNotEmpty(sqlGroup.getList())) {
                            for (int i = 0; i < sqlGroup.getList().size(); i++) {
                                SqlNode selectNode = sqlGroup.getList().get(i);
                                SqlNode replaceNode = replaceNodeInfo(selectNode, mappingTable, tableAlias);
                                sqlGroup.set(i, replaceNode);
                            }
                        }

                        System.out.println("-----------------");
                    }
                }
                else {
                    //TODO
                    System.out.println(sqlNode);
                    throw new RuntimeException("---not deal type:" + sqlNode);
                }

                break;
            default:
                break;
        }
    }

    private SqlNode replaceNodeInfo(SqlNode groupNode, HashBasedTable<String, String, String> mappingTable, String tableAlias)
    {
        if (groupNode.getKind() == IDENTIFIER) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) groupNode;
            String mappingFieldName = mappingTable.get(sqlIdentifier.getComponent(0).getSimple(), sqlIdentifier.getComponent(1).getSimple());
            sqlIdentifier = sqlIdentifier.setName(0, tableAlias);
            return sqlIdentifier.setName(1, mappingFieldName);
        }
        else if (groupNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) groupNode;
            for (int i = 0; i < sqlBasicCall.getOperandList().size(); i++) {
                SqlNode sqlNode = sqlBasicCall.getOperandList().get(i);
                SqlNode replaceNode = replaceSelectFieldName(sqlNode, mappingTable, tableAlias);
                sqlBasicCall.getOperands()[i] = replaceNode;
            }

            return sqlBasicCall;
        }
        else {
            return groupNode;
        }
    }

    public SqlNode filterNodeWithTargetName(SqlNode sqlNode, String targetTableName)
    {

        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case SELECT:
                SqlNode fromNode = ((SqlSelect) sqlNode).getFrom();
                if (fromNode.getKind() == AS && ((SqlBasicCall) fromNode).getOperands()[0].getKind() == IDENTIFIER) {
                    if (((SqlBasicCall) fromNode).getOperands()[0].toString().equalsIgnoreCase(targetTableName)) {
                        return sqlNode;
                    }
                    else {
                        return null;
                    }
                }
                else {
                    return filterNodeWithTargetName(fromNode, targetTableName);
                }
            case AS:
                SqlNode childNode = ((SqlBasicCall) sqlNode).getOperands()[0];
                return filterNodeWithTargetName(childNode, targetTableName);
            case JOIN:
                SqlNode leftNode = ((SqlJoin) sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin) sqlNode).getRight();
                SqlNode leftReturnNode = filterNodeWithTargetName(leftNode, targetTableName);
                SqlNode rightReturnNode = filterNodeWithTargetName(rightNode, targetTableName);

                if (leftReturnNode != null) {
                    return leftReturnNode;
                }
                else if (rightReturnNode != null) {
                    return rightReturnNode;
                }
                else {
                    return null;
                }
        }

        return null;
    }

    public Table getTableFromCache(Map<String, Table> localTableCache, String tableAlias, String tableName)
    {
        Table table = localTableCache.get(tableAlias);
        if (table == null) {
            table = localTableCache.get(tableName);
        }

        if (table == null) {
            throw new RuntimeException("not register table " + tableName);
        }

        return table;
    }

    private List<SqlNode> replaceSelectStarFieldName(SqlNode selectNode, HashBasedTable<String, String, String> mappingTable, String tableAlias)
    {
        SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;
        List<SqlNode> sqlNodes = Lists.newArrayList();
        if (sqlIdentifier.isStar()) {//处理 [* or table.*]
            int identifierSize = sqlIdentifier.names.size();
            Collection<String> columns = null;
            if (identifierSize == 1) {
                columns = mappingTable.values();
            }
            else {
                columns = mappingTable.row(sqlIdentifier.names.get(0)).values();
            }

            for (String colAlias : columns) {
                SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
                List<String> columnInfo = Lists.newArrayList();
                columnInfo.add(tableAlias);
                columnInfo.add(colAlias);
                SqlIdentifier sqlIdentifierAlias = new SqlIdentifier(columnInfo, sqlParserPos);
                sqlNodes.add(sqlIdentifierAlias);
            }

            return sqlNodes;
        }
        else {
            throw new RuntimeException("is not a star select field." + selectNode);
        }
    }

    private SqlNode replaceSelectFieldName(SqlNode selectNode, HashBasedTable<String, String, String> mappingTable, String tableAlias)
    {
        if (selectNode.getKind() == AS) {
            SqlNode leftNode = ((SqlBasicCall) selectNode).getOperands()[0];
            SqlNode replaceNode = replaceSelectFieldName(leftNode, mappingTable, tableAlias);
            if (replaceNode != null) {
                ((SqlBasicCall) selectNode).getOperands()[0] = replaceNode;
            }

            return selectNode;
        }
        else if (selectNode.getKind() == IDENTIFIER) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;

            if (sqlIdentifier.names.size() == 1) {
                return null;
            }

            String mappingFieldName = mappingTable.get(sqlIdentifier.getComponent(0).getSimple(), sqlIdentifier.getComponent(1).getSimple());
            if (mappingFieldName == null) {
                throw new RuntimeException("can't find mapping fieldName:" + selectNode.toString());
            }

            sqlIdentifier = sqlIdentifier.setName(0, tableAlias);
            sqlIdentifier = sqlIdentifier.setName(1, mappingFieldName);
            return sqlIdentifier;
        }
        else if (selectNode.getKind() == LITERAL || selectNode.getKind() == LITERAL_CHAIN) {//字面含义
            return selectNode;
        }
        else if (selectNode.getKind() == OTHER_FUNCTION
                || selectNode.getKind() == DIVIDE
                || selectNode.getKind() == CAST
                || selectNode.getKind() == SUM
                || selectNode.getKind() == AVG
                || selectNode.getKind() == MAX
                || selectNode.getKind() == MIN
                || selectNode.getKind() == TRIM
                || selectNode.getKind() == TIMES
                || selectNode.getKind() == PLUS
                || selectNode.getKind() == IN
                || selectNode.getKind() == OR
                || selectNode.getKind() == AND
                || selectNode.getKind() == COUNT
                || selectNode.getKind() == SUM
                || selectNode.getKind() == SUM0
                || selectNode.getKind() == LEAD
                || selectNode.getKind() == LAG
                || selectNode.getKind() == EQUALS
                || selectNode.getKind() == NOT_EQUALS
                || selectNode.getKind() == MINUS
                || selectNode.getKind() == TUMBLE
                || selectNode.getKind() == TUMBLE_START
                || selectNode.getKind() == TUMBLE_END
                || selectNode.getKind() == SESSION
                || selectNode.getKind() == SESSION_START
                || selectNode.getKind() == SESSION_END
                || selectNode.getKind() == BETWEEN
                || selectNode.getKind() == IS_NULL
                || selectNode.getKind() == IS_NOT_NULL
        ) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) selectNode;
            for (int i = 0; i < sqlBasicCall.getOperands().length; i++) {
                SqlNode sqlNode = sqlBasicCall.getOperands()[i];
                if (sqlNode instanceof SqlLiteral) {
                    continue;
                }

                if (sqlNode instanceof SqlDataTypeSpec) {
                    continue;
                }

                SqlNode replaceNode = replaceSelectFieldName(sqlNode, mappingTable, tableAlias);
                if (replaceNode == null) {
                    continue;
                }

                sqlBasicCall.getOperands()[i] = replaceNode;
            }

            return selectNode;
        }
        else if (selectNode.getKind() == CASE) {
            System.out.println("selectNode");
            SqlCase sqlCase = (SqlCase) selectNode;
            SqlNodeList whenOperands = sqlCase.getWhenOperands();
            SqlNodeList thenOperands = sqlCase.getThenOperands();
            SqlNode elseNode = sqlCase.getElseOperand();

            for (int i = 0; i < whenOperands.size(); i++) {
                SqlNode oneOperand = whenOperands.get(i);
                SqlNode replaceNode = replaceSelectFieldName(oneOperand, mappingTable, tableAlias);
                if (replaceNode != null) {
                    whenOperands.set(i, replaceNode);
                }
            }

            for (int i = 0; i < thenOperands.size(); i++) {
                SqlNode oneOperand = thenOperands.get(i);
                SqlNode replaceNode = replaceSelectFieldName(oneOperand, mappingTable, tableAlias);
                if (replaceNode != null) {
                    thenOperands.set(i, replaceNode);
                }
            }

            replaceSelectFieldName(elseNode, mappingTable, tableAlias);
            return selectNode;
        }
        else if (selectNode.getKind() == OTHER) {
            //不处理
            return selectNode;
        }
        else {
            throw new RuntimeException(String.format("not support node kind of %s to replace name now.", selectNode.getKind()));
        }
    }

    /**
     * Analyzing conditions are very join the dimension tables include all equivalent conditions (i.e., dimension table is the primary key definition
     *
     * @return
     */
    private boolean checkJoinCondition(SqlNode conditionNode, String sideTableAlias, List<String> primaryKeys)
    {

        List<String> conditionFields = getConditionFields(conditionNode, sideTableAlias);
        if (CollectionUtils.isEqualCollection(conditionFields, primaryKeys)) {
            return true;
        }

        return false;
    }

    public List<String> getConditionFields(SqlNode conditionNode, String specifyTableName)
    {
        List<SqlNode> sqlNodeList = Lists.newArrayList();
        if (conditionNode.getKind() == AND) {
            sqlNodeList.addAll(Lists.newArrayList(((SqlBasicCall) conditionNode).getOperands()));
        }
        else {
            sqlNodeList.add(conditionNode);
        }

        List<String> conditionFields = Lists.newArrayList();
        for (SqlNode sqlNode : sqlNodeList) {
            if (sqlNode.getKind() != EQUALS) {
                throw new RuntimeException("not equal operator.");
            }

            SqlIdentifier left = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperands()[0];
            SqlIdentifier right = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperands()[1];

            String leftTableName = left.getComponent(0).getSimple();
            String rightTableName = right.getComponent(0).getSimple();

            String tableCol = "";
            if (leftTableName.equalsIgnoreCase(specifyTableName)) {
                tableCol = left.getComponent(1).getSimple();
            }
            else if (rightTableName.equalsIgnoreCase(specifyTableName)) {
                tableCol = right.getComponent(1).getSimple();
            }
            else {
                throw new RuntimeException(String.format("side table:%s join condition is wrong", specifyTableName));
            }

            conditionFields.add(tableCol);
        }

        return conditionFields;
    }

    public List<FieldInfo> getAllField(JoinScope scope)
    {
        Iterator prefixId = scope.getChildren().iterator();
        List<FieldInfo> fieldInfoList = Lists.newArrayList();
        while (true) {
            JoinScope.ScopeChild resolved;
            RowTypeInfo field;
            if (!prefixId.hasNext()) {
                return fieldInfoList;
            }

            resolved = (JoinScope.ScopeChild) prefixId.next();
            field = resolved.getRowTypeInfo();
            String[] fieldNames = field.getFieldNames();
            TypeInformation<?>[] types = field.getFieldTypes();
            for (int i = 0; i < field.getTotalFields(); i++) {
                String fieldName = fieldNames[i];
                TypeInformation<?> type = types[i];
                FieldInfo fieldInfo = new FieldInfo();
                fieldInfo.setTable(resolved.getAlias());
                fieldInfo.setFieldName(fieldName);
                if (resolved.getRawDataFields() != null) {
                    fieldInfo.setRawFieldName(resolved.getRawDataFields().get(fieldName));
                }
                fieldInfo.setTypeInformation(type);
                fieldInfoList.add(fieldInfo);
            }
        }
    }
}
