package org.dbsyncer.connector.oracle.dialect;

import org.dbsyncer.sdk.connector.database.dialect.DefaultPreparedStatementSetterStrategy;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * oracle PreparedStatementStrategy
 */
public final class OraclePreparedStatementStrategy extends DefaultPreparedStatementSetterStrategy {

    @Override
    public void setValue(PreparedStatement ps, int index, Object value, Integer jdbcType) throws SQLException {
        if (jdbcType == Types.BLOB && value != null) {
            // 必须显式调用 setBinaryStream
            ps.setBinaryStream(index, new ByteArrayInputStream((byte[]) value));
            return;
        }
        super.setValue(ps, index, value, jdbcType);
    }
}
