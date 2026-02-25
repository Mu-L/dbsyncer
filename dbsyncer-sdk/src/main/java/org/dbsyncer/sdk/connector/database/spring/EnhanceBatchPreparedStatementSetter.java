package org.dbsyncer.sdk.connector.database.spring;

import org.dbsyncer.sdk.connector.database.strategy.PreparedStatementSetterStrategy;
import org.dbsyncer.sdk.model.Field;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * BatchPreparedStatementSetter 增强实现
 * <p>
 * 类型增强：通过委托给具体的数据库连接器策略来处理不同数据库类型的参数设置逻辑。
 */
public class EnhanceBatchPreparedStatementSetter extends AbstractEnhancePreparedStatementSetter implements BatchPreparedStatementSetter {

    private final List<Object[]> args;
    private final int[] types;

    public EnhanceBatchPreparedStatementSetter(PreparedStatementSetterStrategy pss, List<Field> fields, List<Map> data) {
        super(pss);
        this.args = getArgs(fields, data);
        this.types = getJdbcTypes(fields, data.get(0));
    }

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        Object[] values = args.get(i);
        for (int colIndex = 0; colIndex < values.length; colIndex++) {
            int jdbcType = types != null && colIndex < types.length ? types[colIndex] : null;
            doSetValue(ps, colIndex + 1, values[colIndex], jdbcType);
        }
    }

    @Override
    public int getBatchSize() {
        return args.size();
    }
}
