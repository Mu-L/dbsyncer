package org.dbsyncer.sdk.connector.database.dialect;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.database.strategy.PreparedStatementSetterStrategy;

import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 默认的PreparedStatementSetter
 */
public class DefaultPreparedStatementSetterStrategy implements PreparedStatementSetterStrategy {

    @Override
    public void setValue(PreparedStatement ps, int index, Object value, Integer jdbcType) throws SdkException {
        try {
            if (value instanceof SqlParameterValue) {
                SqlParameterValue paramValue = (SqlParameterValue) value;
                StatementCreatorUtils.setParameterValue(ps, index, paramValue, paramValue.getValue());
            } else {
                int sqlType = jdbcType != null ? jdbcType : SqlTypeValue.TYPE_UNKNOWN;
                StatementCreatorUtils.setParameterValue(ps, index, sqlType, value);
            }
        } catch (SQLException e) {
            throw new SdkException("设置SQL参数值异常：" + e.getLocalizedMessage(), e);
        }
    }
}
