/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector.database;

import net.sf.jsqlparser.statement.alter.Alter;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.config.SqlBuilderConfig;
import org.dbsyncer.sdk.enums.SqlBuilderEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.PageSql;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.plugin.ReaderContext;

import java.util.List;
import java.util.stream.Collectors;

public interface Database {

    /**
     * 获取dbs唯一标识码
     */
    default String generateUniqueCode() {
        return StringUtil.EMPTY;
    }

    /**
     * 获取引号（默认不加）
     */
    default String buildSqlWithQuotation() {
        return StringUtil.EMPTY;
    }

    /**
     * 返回带引号的名称
     */
    default String buildWithQuotation(String name) {
        return buildSqlWithQuotation() + name + buildSqlWithQuotation();
    }

    /**
     * 获取主键字段名称
     */
    default List<String> buildPrimaryKeys(List<String> primaryKeys) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return primaryKeys;
        }
        return primaryKeys.stream().map(this::buildWithQuotation).collect(Collectors.toList());
    }

    /**
     * 追加主键和参数占位符
     *
     * @param sql
     * @param primaryKeys
     */
    default void appendPrimaryKeys(StringBuilder sql, List<String> primaryKeys) {
        if (CollectionUtils.isEmpty(primaryKeys)) {
            return;
        }
        List<String> pks = primaryKeys.stream().map(name->buildWithQuotation(name) + "=?").collect(Collectors.toList());
        sql.append(StringUtil.join(pks, " AND "));
    }

    /**
     * 获取分页SQL
     *
     * @param config
     * @return
     */
    String getPageSql(PageSql config);

    /**
     * 获取分页参数
     *
     * @param context
     * @return
     */
    Object[] getPageArgs(ReaderContext context);

    /**
     * 获取游标分页SQL
     *
     * @param pageSql
     * @return
     */
    String getPageCursorSql(PageSql pageSql);

    /**
     * 获取游标分页参数
     *
     * @param context
     * @return
     */
    Object[] getPageCursorArgs(ReaderContext context);

    /**
     * @param connectorType 连接器类型名，与 {@link org.dbsyncer.sdk.spi.ConnectorService#getConnectorType()} 一致（如 MySQL）
     */
    boolean supportsConnectorType(String connectorType);

    /**
     * 构建 MODIFY / ALTER COLUMN 语句，使目标列物理类型与 {@code sourceDefinition} 对齐。
     *
     * @param targetInstance     目标连接实例（含 catalog/schema 上下文）
     * @param task               校验任务（库名、构架名）
     * @param targetTableName    目标表名（未加引号）
     * @param targetColumnName   目标列名（未加引号）
     * @param sourceDefinition   源端字段元数据，作为期望定义
     * @param database           目标连接器 {@link Database}（引号规则）
     * @return 可执行的 DDL（不含 SDK 写入 DDL 时追加的唯一标识前缀，由连接器统一处理）
     */
    String buildModifyColumnSql(DatabaseConnectorInstance targetInstance, ValidateSyncTask task,
                                String targetTableName, String targetColumnName,
                                Field sourceDefinition, Database database);

    /**
     * 健康检查
     *
     * @return
     */
    default String getValidationQuery() {
        return "select 1";
    }

    /**
     * 获取查询总数SQL
     */
    default String getQueryCountSql(SqlBuilderConfig sqlConfig) {
        return SqlBuilderEnum.QUERY_COUNT.getSqlBuilder().buildSql(sqlConfig);
    }

    /**
     * 生成upsert
     */
    String buildUpsertSql(DatabaseConnectorInstance connectorInstance, SqlBuilderConfig config);

    /**
     * 生成insert
     */
    default String buildInsertSql(SqlBuilderConfig config) {
        return SqlBuilderEnum.INSERT.getSqlBuilder().buildSql(config);
    }

    default boolean buildCustom(List<String> fs, Field field) {
        return false;
    }

    /**
     * 为特殊字段类型构建自定义的值表达式
     * 
     * <p>用于 INSERT/UPDATE 语句的 VALUES 部分，允许数据库连接器为特定字段类型（如 geometry、geography）
     * 提供自定义的 SQL 表达式，而不是简单的占位符 ?</p>
     * 
     * <p>例如 SQL Server 的 geometry 类型需要使用 geometry::STGeomFromText(?, ?) 来转换</p>
     * 
     * @param vs 值表达式列表（VALUES 部分）
     * @param field 字段信息
     * @return true 表示已添加自定义值表达式，false 表示使用默认的 ? 占位符
     */
    default boolean buildCustomValue(List<String> vs, Field field) {
        return false;
    }

    /**
     * 替换ddl语句中的catalog
     *
     * @param connectorInstance
     * @param alter
     * @return
     */
    default String buildAlterCatalog(DatabaseConnectorInstance connectorInstance, Alter alter){
        return alter.toString();
    }
}