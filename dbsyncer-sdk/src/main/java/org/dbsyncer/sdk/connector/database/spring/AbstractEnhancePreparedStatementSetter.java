package org.dbsyncer.sdk.connector.database.spring;

import org.dbsyncer.sdk.connector.database.strategy.PreparedStatementSetterStrategy;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.CustomData;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * PreparedStatementSetter 增强抽象类
 * <p>
 * 抽象类，实现获取参数类型和参数值
 **/
public abstract class AbstractEnhancePreparedStatementSetter {

    private final PreparedStatementSetterStrategy strategy;

    public AbstractEnhancePreparedStatementSetter(PreparedStatementSetterStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * 设置 PreparedStatement 的参数值
     *
     * @param ps       PreparedStatement 对象
     * @param index    参数索引位置
     * @param value    参数值
     * @param jdbcType jdbc类型，{@link java.sql.Types}
     */
    protected void doSetValue(PreparedStatement ps, int index, Object value, Integer jdbcType) throws SQLException {
        strategy.setValue(ps, index, value, jdbcType);
    }

    /**
     * 获取jdbc参数类型
     *
     * @param fields 字段
     * @param row    数据行
     * @return 参数
     */
    protected int[] getJdbcTypes(List<Field> fields, Map row) {
        List<Integer> types = new ArrayList<>();
        for (Field f : fields) {
            Object val = row.get(f.getName());
            if (val instanceof CustomData) {
                CustomData cd = (CustomData) val;
                Collection<?> extendedData = cd.apply();
                types.addAll(Collections.nCopies(extendedData.size(), f.getType()));
                continue;
            }
            types.add(f.getType());
        }
        return types.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * 批量获取参数
     *
     * @param fields 字段
     * @param data   数据
     * @return 参数
     */
    protected List<Object[]> getArgs(List<Field> fields, List<Map> data) {
        return data.stream().map(row -> getArgs(fields, row)).collect(Collectors.toList());
    }

    /**
     * 获取参数
     *
     * @param fields 字段
     * @param row    数据
     * @return 参数
     */
    protected Object[] getArgs(List<Field> fields, Map row) {
        List<Object> args = new ArrayList<>();
        for (Field f : fields) {
            Object val = row.get(f.getName());
            if (val instanceof CustomData) {
                CustomData cd = (CustomData) val;
                args.addAll(cd.apply());
                continue;
            }
            args.add(val);
        }
        return args.toArray();
    }

}
