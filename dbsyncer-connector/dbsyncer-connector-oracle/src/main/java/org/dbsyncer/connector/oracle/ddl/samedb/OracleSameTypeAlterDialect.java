package org.dbsyncer.connector.oracle.ddl.samedb;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.connector.database.Database;
import org.dbsyncer.sdk.connector.database.DatabaseConnectorInstance;
import org.dbsyncer.sdk.ddl.samedb.AbstractSameTypeAlterDialect;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.ValidateSyncTask;

import java.util.Locale;

/**
 * Oracle：ALTER TABLE ... MODIFY ({@code column type})
 */
public class OracleSameTypeAlterDialect extends AbstractSameTypeAlterDialect {

    private static final String ORACLE = "Oracle";

    @Override
    public boolean supportsConnectorType(String connectorType) {
        return equalsIgnoreCase(connectorType, ORACLE);
    }

    @Override
    public String buildModifyColumnSql(DatabaseConnectorInstance targetInstance, ValidateSyncTask task,
                                       String targetTableName, String targetColumnName,
                                       Field sourceDefinition, Database database) {
        String qualifiedTable = qualifyTable(targetInstance, task, targetTableName, database);
        String col = database.buildWithQuotation(targetColumnName);
        String type = formatPhysicalType(sourceDefinition);
        return String.format(Locale.ROOT, "ALTER TABLE %s MODIFY (%s %s)", qualifiedTable, col, type);
    }

    private String qualifyTable(DatabaseConnectorInstance targetInstance, ValidateSyncTask task,
                               String tableName, Database database) {
        String schema = StringUtil.isNotBlank(task.getTargetSchema())
                ? task.getTargetSchema()
                : targetInstance.getCatalog();
        if (StringUtil.isBlank(schema)) {
            return database.buildWithQuotation(tableName);
        }
        return database.buildWithQuotation(schema) + "." + database.buildWithQuotation(tableName);
    }
}
