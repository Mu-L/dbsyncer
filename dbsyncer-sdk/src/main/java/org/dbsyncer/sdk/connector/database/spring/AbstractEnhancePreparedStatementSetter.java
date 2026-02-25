package org.dbsyncer.sdk.connector.database.spring;

import org.dbsyncer.sdk.SdkException;
import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;

import org.jspecify.annotations.Nullable;

import java.sql.PreparedStatement;

/**
 * PreparedStatementSetter 增强抽象类
 * <p>
 * 抽象类，提供数据库连接器策略工厂，用于获取数据库连接器策略。
 **/
public abstract class AbstractEnhancePreparedStatementSetter {

    protected final AbstractDatabaseConnector connector;

    public AbstractEnhancePreparedStatementSetter(AbstractDatabaseConnector connector) {
        this.connector = connector;
    }

    /**
     * 设置 PreparedStatement 的参数值
     *
     * @param ps       PreparedStatement 对象
     * @param index    参数索引位置
     * @param value    参数值
     * @param jdbcType jdbc类型，{@link java.sql.Types}
     */
    protected void doSetValue(PreparedStatement ps, int index, Object value, @Nullable Integer jdbcType) throws SdkException {
        connector.getPreparedStatementSetterStrategy().setValue(ps, index, value, jdbcType);
    }
}
