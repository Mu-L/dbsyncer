/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import org.dbsyncer.common.enums.CommonTaskTriggerEnum;

import java.util.List;

public class ValidateSyncTask extends CommonTask {
    // 数据源连接器ID
    private String sourceConnectorId;

    // 数据源库名称
    private String sourceDatabase;

    // 数据源库构架名
    private String sourceSchema;

    // 数据源库表列表
    private List<Table> sourceTable;

    // 目标源连接器ID
    private String targetConnectorId;

    // 目标源库名称
    private String targetDatabase;

    // 目标源库构架名
    private String targetSchema;

    // 目标源库表列表
    private List<Table> targetTable;

    /**
     * 触发方式
     */
    private String trigger = CommonTaskTriggerEnum.ONCE.getCode();

    /**
     * 定时表达式, 格式: [秒] [分] [小时] [日] [月] [周]
     */
    private String cron = "*/30 * * * * ?";

    /**
     * 校验不一致后是否同步目标库
     */
    private boolean enableSync = false;

    /**
     * 校验范围（行数据）
     */
    private boolean enablerRowData = true;

    /**
     * 校验范围（索引）
     */
    private boolean enableIndex = false;

    /**
     * 校验范围（触发器）
     */
    private boolean enableTrigger = false;

    /**
     * 校验范围（函数）
     */
    private boolean enableFunction = false;

    public String getSourceConnectorId() {
        return sourceConnectorId;
    }

    public void setSourceConnectorId(String sourceConnectorId) {
        this.sourceConnectorId = sourceConnectorId;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(String sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    public List<Table> getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(List<Table> sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getTargetConnectorId() {
        return targetConnectorId;
    }

    public void setTargetConnectorId(String targetConnectorId) {
        this.targetConnectorId = targetConnectorId;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    public String getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(String targetSchema) {
        this.targetSchema = targetSchema;
    }

    public List<Table> getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(List<Table> targetTable) {
        this.targetTable = targetTable;
    }

    public String getTrigger() {
        return trigger;
    }

    public void setTrigger(String trigger) {
        this.trigger = trigger;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public boolean isEnableSync() {
        return enableSync;
    }

    public void setEnableSync(boolean enableSync) {
        this.enableSync = enableSync;
    }

    public boolean isEnablerRowData() {
        return enablerRowData;
    }

    public void setEnablerRowData(boolean enablerRowData) {
        this.enablerRowData = enablerRowData;
    }

    public boolean isEnableIndex() {
        return enableIndex;
    }

    public void setEnableIndex(boolean enableIndex) {
        this.enableIndex = enableIndex;
    }

    public boolean isEnableTrigger() {
        return enableTrigger;
    }

    public void setEnableTrigger(boolean enableTrigger) {
        this.enableTrigger = enableTrigger;
    }

    public boolean isEnableFunction() {
        return enableFunction;
    }

    public void setEnableFunction(boolean enableFunction) {
        this.enableFunction = enableFunction;
    }
}
