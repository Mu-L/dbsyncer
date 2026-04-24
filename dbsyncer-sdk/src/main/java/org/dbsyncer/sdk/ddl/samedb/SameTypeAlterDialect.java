package org.dbsyncer.sdk.ddl.samedb;

import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.ValidateSyncTask;

/**
 * 同构场景下「按源端字段定义修改目标列」的方言策略。
 * <p>
 * 异构库（如 MySQL→SqlServer）可在后续新增 {@code HeterologousAlterDialect} 或映射层，不在此接口范围内。
 */
public interface SameTypeAlterDialect {

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
}
