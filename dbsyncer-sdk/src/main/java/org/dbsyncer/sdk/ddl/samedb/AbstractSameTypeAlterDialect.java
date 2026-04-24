package org.dbsyncer.sdk.ddl.samedb;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.Field;

import java.util.Locale;

/**
 * 根据 {@link Field} 拼装物理类型片段的通用逻辑（同构同源端 JDBC 元数据）。
 */
public abstract class AbstractSameTypeAlterDialect implements SameTypeAlterDialect {

    /**
     * 生成列定义中的类型片段（不含列名）。
     *
     * @param sourceDefinition 源端字段
     */
    protected String formatPhysicalType(Field sourceDefinition) {
        String raw = sourceDefinition.getTypeName();
        if (StringUtil.isBlank(raw)) {
            return defaultVarchar();
        }
        String t = raw.trim().toUpperCase(Locale.ROOT);
        int size = Math.max(0, sourceDefinition.getColumnSize());
        int ratio = Math.max(0, sourceDefinition.getRatio());

        if ("BIT".equals(t)) {
            return "BIT";
        }

        if ("DECIMAL".equals(t) || "NUMERIC".equals(t)) {
            int p = size <= 0 ? 38 : size;
            return String.format(Locale.ROOT, "%s(%d,%d)", t, p, ratio);
        }

        if (typeNeedsPrecisionScale(t)) {
            int p = size <= 0 ? (t.contains("DOUBLE") ? 53 : 24) : size;
            return String.format(Locale.ROOT, "%s(%d)", t, p);
        }

        if (typeNeedsLength(t)) {
            int len = size > 0 ? size : defaultLengthForCharFamily(t);
            return String.format(Locale.ROOT, "%s(%d)", t, len);
        }

        if (size > 0 && (t.contains("TIMESTAMP") || "TIME".equals(t))) {
            if (ratio > 0) {
                return String.format(Locale.ROOT, "%s(%d)", t, Math.min(ratio, 6));
            }
            return String.format(Locale.ROOT, "%s(%d)", t, size);
        }

        return t;
    }

    /**
     * 方言可覆盖：无类型名时的默认类型。
     */
    protected String defaultVarchar() {
        return "VARCHAR(255)";
    }

    private int defaultLengthForCharFamily(String t) {
        if (t.contains("TEXT") || "CLOB".equals(t) || "NCLOB".equals(t)) {
            return 65535;
        }
        return 255;
    }

    private boolean typeNeedsLength(String t) {
        return t.contains("CHAR") && !t.contains("BINARY")
                || t.contains("BINARY")
                || "BIT".equals(t);
    }

    private boolean typeNeedsPrecisionScale(String t) {
        return t.contains("FLOAT") || "REAL".equals(t) || "DOUBLE".equals(t);
    }

    protected boolean equalsIgnoreCase(String connectorType, String expected) {
        return expected != null && expected.equalsIgnoreCase(StringUtil.trim(connectorType));
    }
}
