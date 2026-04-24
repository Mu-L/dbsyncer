package org.dbsyncer.connector.sqlserver.ddl.samedb;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.ddl.samedb.AbstractSameTypeAlterDialect;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.ValidateSyncTask;

import java.util.Locale;

/**
 * SQL Server：ALTER TABLE ... ALTER COLUMN ...
 */
public class SqlServerSameTypeAlterDialect extends AbstractSameTypeAlterDialect {

    private static final String SQLSERVER = "SqlServer";

    @Override
    public boolean supportsConnectorType(String connectorType) {
        return equalsIgnoreCase(connectorType, SQLSERVER);
    }

    @Override
    public String buildModifyColumnSql(DatabaseConnectorInstance targetInstance, ValidateSyncTask task,
                                       String targetTableName, String targetColumnName,
                                       Field sourceDefinition, Database database) {
        String qualifiedTable = qualifyTable(task, targetTableName, database);
        String col = database.buildWithQuotation(targetColumnName);
        String type = formatPhysicalType(sourceDefinition);
        return String.format(Locale.ROOT, "ALTER TABLE %s ALTER COLUMN %s %s", qualifiedTable, col, type);
    }

    /**
     * 可选库前缀 {@code [db].[schema].[table]}；仅 schema + 表名亦可。
     */
    private String qualifyTable(ValidateSyncTask task, String tableName, Database database) {
        String schema = StringUtil.isNotBlank(task.getTargetSchema()) ? task.getTargetSchema() : "dbo";
        String schemaTable = database.buildWithQuotation(schema) + "." + database.buildWithQuotation(tableName);
        if (StringUtil.isBlank(task.getTargetDatabase())) {
            return schemaTable;
        }
        return "[" + task.getTargetDatabase() + "]." + schemaTable;
    }
}
