package org.dbsyncer.sdk.connector.database.spring;

import org.dbsyncer.sdk.connector.database.AbstractDatabaseConnector;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.PreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * PreparedStatementSetter 增强实现
 * <p>
 * 类型增强：通过委托给具体的数据库连接器策略来处理不同数据库类型的参数设置逻辑。
 **/
public class EnhancePreparedStatementSetter extends AbstractEnhancePreparedStatementSetter implements PreparedStatementSetter {

    private final Object[] args;
    private final int[] types;

    public EnhancePreparedStatementSetter(AbstractDatabaseConnector connector, Object[] args, @Nullable int[] types) {
        super(connector);
        this.args = args;
        this.types = types;
    }
    @Override
    public void setValues(PreparedStatement ps) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            int jdbcType = types != null && i < types.length ? types[i] : null;
            doSetValue(ps, i + 1, args[i], jdbcType);
        }
    }
}
