package org.dbsyncer.sdk.connector.database.spring;

import org.dbsyncer.sdk.connector.database.strategy.PreparedStatementSetterStrategy;
import org.dbsyncer.sdk.model.Field;
import org.springframework.jdbc.core.PreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * PreparedStatementSetter 增强实现
 * <p>
 * 类型增强：通过委托给具体的数据库连接器策略来处理不同数据库类型的参数设置逻辑。
 **/
public class EnhancePreparedStatementSetter extends AbstractEnhancePreparedStatementSetter implements PreparedStatementSetter {

    private final Object[] args;
    private final int[] types;

    public EnhancePreparedStatementSetter(PreparedStatementSetterStrategy pss, List<Field> fields, Map row) {
        super(pss);
        this.args = getArgs(fields, row);
        this.types = getJdbcTypes(fields, row);
    }
    @Override
    public void setValues(PreparedStatement ps) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            int jdbcType = types != null && i < types.length ? types[i] : null;
            doSetValue(ps, i + 1, args[i], jdbcType);
        }
    }
}
