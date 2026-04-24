package org.dbsyncer.connector.postgresql.ddl.samedb;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.ddl.samedb.AbstractSameTypeAlterDialect;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.ValidateSyncTask;

import java.util.Locale;

/**
 * PostgreSQL：ALTER TABLE ... ALTER COLUMN ... TYPE ...
 */
public class PostgreSqlSameTypeAlterDialect extends AbstractSameTypeAlterDialect {

    private static final String PG = "PostgreSQL";

    @Override
    public boolean supportsConnectorType(String connectorType) {
        return equalsIgnoreCase(connectorType, PG);
    }

    @Override
    public String buildModifyColumnSql(DatabaseConnectorInstance targetInstance, ValidateSyncTask task,
                                       String targetTableName, String targetColumnName,
                                       Field sourceDefinition, Database database) {
        String qualifiedTable = qualifyTable(task, targetTableName, database);
        String col = database.buildWithQuotation(targetColumnName);
        String type = formatPhysicalType(sourceDefinition);
        return String.format(Locale.ROOT, "ALTER TABLE %s ALTER COLUMN %s TYPE %s", qualifiedTable, col, type);
    }

    private String qualifyTable(ValidateSyncTask task, String tableName, Database database) {
        String schema = StringUtil.isNotBlank(task.getTargetSchema()) ? task.getTargetSchema() : "public";
        return database.buildWithQuotation(schema) + "." + database.buildWithQuotation(tableName);
    }
}
