package org.dbsyncer.connector.oracle.dialect;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.database.dialect.DefaultPreparedStatementSetterStrategy;

import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * oracle PreparedStatementStrategy
 */
public class OraclePreparedStatementStrategy extends DefaultPreparedStatementSetterStrategy {

    @Override
    public void setValue(PreparedStatement ps, int index, Object value, @Nullable Integer jdbcType) throws SdkException {
        try {
            boolean useType = jdbcType != null;
            if (useType && value != null) {
                if(jdbcType == Types.BLOB) {
                    // 必须显式调用 setBinaryStream
                    ps.setBinaryStream(index, new ByteArrayInputStream((byte[]) value));
                } else {
                    // CLOB、NCLOB无需特殊处理，只要类型传入正确即可
                    super.setValue(ps, index, value, jdbcType);
                }
            } else {
                super.setValue(ps, index, value, jdbcType);
            }
        } catch (SQLException e) {
            throw new SdkException("设置SQL参数值异常：" + e.getLocalizedMessage(), e);
        }
    }
}
