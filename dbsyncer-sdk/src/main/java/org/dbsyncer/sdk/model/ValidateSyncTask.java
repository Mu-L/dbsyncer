/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

public class ValidateSyncTask extends CommonTask {
    private String sourceConnectorId;
    private String targetConnectorId;
    private String sourceDatabase;
    private String targetDatabase;
    private String name;
    private String tableGroups;

    /**
     * 触发方式, 可选值: once, cron
     */
    private String trigger = "once";

    /**
     * 定时表达式, 格式: [秒] [分] [小时] [日] [月] [周]
     */
    private String cron = "*/30 * * * * ?";

    public String getSourceConnectorId() {
        return sourceConnectorId;
    }

    public void setSourceConnectorId(String sourceConnectorId) {
        this.sourceConnectorId = sourceConnectorId;
    }

    public String getTargetConnectorId() {
        return targetConnectorId;
    }

    public void setTargetConnectorId(String targetConnectorId) {
        this.targetConnectorId = targetConnectorId;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public String getTableGroups() {
        return tableGroups;
    }

    public void setTableGroups(String tableGroups) {
        this.tableGroups = tableGroups;
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
}
