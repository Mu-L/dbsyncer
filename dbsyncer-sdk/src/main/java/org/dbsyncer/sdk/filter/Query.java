/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.filter;

import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.impl.IntFilter;
import org.dbsyncer.sdk.filter.impl.StringFilter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-08 15:17
 */
public class Query {

    /**
     * {@link StorageEnum}
     */
    private StorageEnum type;

    private String metaId;

    private BooleanFilter booleanFilter = new BooleanFilter();

    /**
     * 查询应用性能，不用排序查询，只用查询总量即可
     */
    private boolean queryTotal;

    private int pageNum = 1;

    private int pageSize = 20;

    /**
     * 修改时间和创建默认降序返回
     */
    private SortEnum sort = SortEnum.DESC;

    /**
     * 自定义排序字段列表，非空时优先使用，为空时走默认排序逻辑
     */
    private List<OrderBy> orderByList = new ArrayList<>();

    /**
     * SELECT 白名单 查询包含白名单的字段
     */
    private List<String> includeSelectLabels;

    /**
     * SELECT 黑名单：排除掉黑名单的字段
     */
    private final List<String> excludeSelectLabels = new ArrayList<>();

    /**
     * 返回值转换器，限Disk使用
     */
    private Map<String, FieldResolver> fieldResolverMap = new ConcurrentHashMap<>();

    public Query() {
    }

    public Query(int pageNum, int pageSize) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
    }

    public void addFilter(String name, String value) {
        booleanFilter.add(new StringFilter(name, FilterEnum.EQUAL, value, false));
    }

    public void addFilter(String name, String value, boolean enableHighLightSearch) {
        booleanFilter.add(new StringFilter(name, FilterEnum.LIKE, value, enableHighLightSearch));
    }

    public void addFilter(String name, int value) {
        booleanFilter.add(new IntFilter(name, value));
    }

    public StorageEnum getType() {
        return type;
    }

    public void setType(StorageEnum type) {
        this.type = type;
    }

    public String getMetaId() {
        return metaId;
    }

    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }

    public BooleanFilter getBooleanFilter() {
        return booleanFilter;
    }

    public void setBooleanFilter(BooleanFilter booleanFilter) {
        this.booleanFilter = booleanFilter;
    }

    public boolean isQueryTotal() {
        return queryTotal;
    }

    public void setQueryTotal(boolean queryTotal) {
        this.queryTotal = queryTotal;
    }

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public SortEnum getSort() {
        return sort;
    }

    public void setSort(SortEnum sort) {
        this.sort = sort;
    }

    public List<OrderBy> getOrderByList() {
        return orderByList;
    }

    /**
     * 添加自定义排序字段（使用指定排序方向）
     *
     * @param fieldName 字段名（驼峰格式，如 createTime）
     * @param sort      排序方向
     */
    public void addOrderBy(String fieldName, SortEnum sort) {
        orderByList.add(new OrderBy(fieldName, sort));
    }

    /**
     * 添加自定义排序字段（使用 Query 全局排序方向）
     *
     * @param fieldName 字段名（驼峰格式，如 createTime）
     */
    public void addOrderBy(String fieldName) {
        orderByList.add(new OrderBy(fieldName, null));
    }

    public boolean hasCustomOrderBy() {
        return !orderByList.isEmpty();
    }

    /**
     * 是否对当前查询使用自定义 SELECT 列（白名单或黑名单）。
     */
    public boolean hasSelectProjection() {
        return hasIncludeSelectLabels() || !excludeSelectLabels.isEmpty();
    }

    public boolean hasIncludeSelectLabels() {
        return includeSelectLabels != null && !includeSelectLabels.isEmpty();
    }

    /**
     * 仅查询指定 label 对应的列；与 {@link #addExcludeSelectLabel} 同时配置时以本列表为准。
     *
     * @param labels 字段 label，驼峰形式，如 {@code id}、{@code content}
     */
    public void setIncludeSelectLabels(List<String> labels) {
        if (labels == null || labels.isEmpty()) {
            this.includeSelectLabels = null;
            return;
        }
        this.includeSelectLabels = new ArrayList<>(labels.size());
        for (String label : labels) {
            if (label != null && !label.isEmpty()) {
                this.includeSelectLabels.add(label);
            }
        }
        if (this.includeSelectLabels.isEmpty()) {
            this.includeSelectLabels = null;
        }
    }

    public List<String> getIncludeSelectLabels() {
        return includeSelectLabels == null ? Collections.emptyList() : Collections.unmodifiableList(includeSelectLabels);
    }

    /**
     * 从默认全列 SELECT 中排除指定 label（如大字段 {@code content}）。
     *
     * @param labelName 字段 label，驼峰形式
     */
    public void addExcludeSelectLabel(String labelName) {
        if (labelName == null || labelName.isEmpty()) {
            return;
        }
        excludeSelectLabels.add(labelName);
    }

    public List<String> getExcludeSelectLabels() {
        return Collections.unmodifiableList(excludeSelectLabels);
    }

    public Map<String, FieldResolver> getFieldResolverMap() {
        return fieldResolverMap;
    }

    public void setFieldResolverMap(Map<String, FieldResolver> fieldResolverMap) {
        this.fieldResolverMap = fieldResolverMap;
    }

    /**
     * 排序字段描述，支持每个字段独立指定排序方向
     */
    public static class OrderBy {

        private final String fieldName;

        /**
         * 为 null 时跟随 Query 全局排序方向
         */
        private final SortEnum sort;

        public OrderBy(String fieldName, SortEnum sort) {
            this.fieldName = fieldName;
            this.sort = sort;
        }

        public String getFieldName() {
            return fieldName;
        }

        public SortEnum getSort() {
            return sort;
        }
    }
}
