package org.dbsyncer.sdk.connector.database.strategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 设置 PreparedStatement 参数的接口
 */
public interface PreparedStatementSetterStrategy {

    /**
     * 设置 PreparedStatement 的参数值
     *
     * @param ps       PreparedStatement 对象
     * @param index    参数索引位置
     * @param value    参数值
     * @param jdbcType jdbc类型，{@link java.sql.Types}
     */
    void setValue(PreparedStatement ps, int index, Object value, Integer jdbcType) throws SQLException;
}