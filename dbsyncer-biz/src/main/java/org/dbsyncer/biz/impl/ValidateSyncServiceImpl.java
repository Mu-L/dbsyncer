/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.ValidateSyncService;
import org.dbsyncer.biz.checker.impl.tablegroup.ValidateSyncTableGroupChecker;
import org.dbsyncer.biz.vo.ValidateSyncTaskVO;
import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.CommonTaskTriggerEnum;
import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.base.ConnectorFactory;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.util.ConnectorInstanceUtil;
import org.dbsyncer.parser.util.ConnectorServiceContextUtil;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.DefaultConnectorServiceContext;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class ValidateSyncServiceImpl implements ValidateSyncService {

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private TaskService<ValidateSyncTask> taskService;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private TableGroupService tableGroupService;

    @Resource
    private ConnectorFactory connectorFactory;

    @Resource
    private LogService logService;

    @Resource
    private PreloadTemplate preloadTemplate;

    @Resource
    private ValidateSyncTableGroupChecker validateSyncTableGroupChecker;

    /**
     * 任务启停锁
     */
    private final static Object LOCK = new Object();

    @Override
    public ValidateSyncTaskVO get(String id) {
        return convertTask2Vo(taskService.get(id));
    }

    @Override
    public String add(Map<String, String> params) {
        ValidateSyncTask task = new ValidateSyncTask();
        checkTask(task, params);
        // 默认检查行数据
        task.setEnablerRowData(true);
        // 关联同步任务
        String mappingId = params.get("mappingId");
        if (StringUtil.isNotBlank(mappingId)) {
            Mapping mapping = profileComponent.getMapping(mappingId);
            Assert.notNull(mapping, "mapping is not exist");
            task.setSourceConnectorId(mapping.getSourceConnectorId());
            task.setSourceDatabase(mapping.getSourceDatabase());
            task.setSourceSchema(mapping.getSourceSchema());
            task.setSourceTable(deepCopy(mapping.getSourceTable()));
            task.setTargetConnectorId(mapping.getTargetConnectorId());
            task.setTargetDatabase(mapping.getTargetDatabase());
            task.setTargetSchema(mapping.getTargetSchema());
            task.setTargetTable(deepCopy(mapping.getTargetTable()));
            // 复制表组列表
            List<TableGroup> tableGroupAll = tableGroupService.getTableGroupAll(mappingId);
            if (!CollectionUtils.isEmpty(tableGroupAll)) {
                tableGroupAll.forEach(tableGroup -> {
                    TableGroup newTable = deepCopy(tableGroup);
                    newTable.setId(String.valueOf(snowflakeIdWorker.nextId()));
                    newTable.setMappingId(task.getId());
                    profileComponent.addTableGroup(newTable);
                });
            }
        } else {
            task.setSourceConnectorId(params.get("sourceConnectorId"));
            task.setSourceDatabase(params.get("sourceDatabase"));
            task.setSourceSchema(params.get("sourceSchema"));
            task.setTargetConnectorId(params.get("targetConnectorId"));
            task.setTargetDatabase(params.get("targetDatabase"));
            task.setTargetSchema(params.get("targetSchema"));
            // TODO 解析表组
            //  params.get("tableGroups")
            // 匹配相似表 on
            // StringUtil.isNotBlank(params.get("autoMatchTable"));
        }
        String id = taskService.add(task);
        preloadTemplate.reConnect(task);
        return id;
    }

    private List<Table> deepCopy(List<Table> targetTable) {
        return JsonUtil.jsonToArray(JsonUtil.objToJson(targetTable), Table.class);
    }

    private TableGroup deepCopy(TableGroup tableGroup) {
        return JsonUtil.jsonToObj(JsonUtil.objToJson(tableGroup), TableGroup.class);
    }

    @Override
    public String edit(Map<String, String> params) {
        ValidateSyncTask task = taskService.get(params.get("id"));
        if (task == null) {
            throw new IllegalArgumentException("Task not found");
        }
        checkTask(task, params);
        List<TableGroup> groupAll = profileComponent.getTableGroupAll(task.getId());
        if (!CollectionUtils.isEmpty(groupAll)) {
            // 手动排序
            String[] sortedTableGroupIds = StringUtil.split(params.get("sortedTableGroupIds"), StringUtil.VERTICAL_LINE);
            if (null != sortedTableGroupIds && sortedTableGroupIds.length > 0) {
                Map<String, TableGroup> tableGroupMap = groupAll.stream().collect(Collectors.toMap(TableGroup::getId, f->f, (k1, k2)->k1));
                groupAll.clear();
                int size = sortedTableGroupIds.length;
                int i = size;
                while (i > 0) {
                    TableGroup g = tableGroupMap.get(sortedTableGroupIds[size - i]);
                    Assert.notNull(g, "Invalid sorted tableGroup.");
                    g.setIndex(i);
                    groupAll.add(g);
                    i--;
                }
            }
            for (TableGroup g : groupAll) {
                profileComponent.editConfigModel(g);
            }
        }
        return taskService.edit(task);
    }

    @Override
    public String copy(String id) {
        ValidateSyncTask task = taskService.get(id);
        Assert.notNull(task, "Task not found");
        String json = JsonUtil.objToJson(task);
        ValidateSyncTask newTask = JsonUtil.jsonToObj(json, ValidateSyncTask.class);
        newTask.setId(String.valueOf(snowflakeIdWorker.nextId()));
        newTask.setName(newTask.getName() + "(复制)");
        newTask.setStatus(CommonTaskStatusEnum.READY.getCode());
        newTask.setType(CommonTaskTypeEnum.VALIDATE_SYNC.name());
        newTask.setUpdateTime(System.currentTimeMillis());
        String newId = taskService.add(newTask);
        preloadTemplate.reConnect(newTask);
        return newId;
    }

    @Override
    public String delete(String id) {
        // TODO 任务状态判断，运行中不允许删除表组
        // 删除tableGroup
        List<TableGroup> groupList = profileComponent.getTableGroupAll(id);
        if (!CollectionUtils.isEmpty(groupList)) {
            groupList.forEach(t -> profileComponent.removeTableGroup(t.getId()));
        }

        taskService.delete(id);
        return "删除成功";
    }

    @Override
    public String start(String id) {
        taskService.start(id);
        return "启动成功";
    }

    @Override
    public String stop(String id) {
        taskService.stop(id);
        return "停止成功";
    }

    @Override
    public Paging<ValidateSyncTaskVO> search(Map<String, String> params) {
        Paging search = taskService.search(params);
        Collection data = search.getData();
        if (CollectionUtils.isEmpty(data)) {
            return search;
        }
        List<ValidateSyncTaskVO> list = new ArrayList<>();
        data.forEach(task -> {
            if (task instanceof ValidateSyncTask) {
                ValidateSyncTask t = (ValidateSyncTask) task;
                ValidateSyncTaskVO vo = convertTask2Vo(t);
                if (vo != null) {
                    list.add(vo);
                }
            }
        });
        search.setData(list);
        return search;
    }

    @Override
    public Paging<TableGroup> searchTableGroup(Map<String, String> params) {
        String id = params.get("id");
        ValidateSyncTask task = taskService.get(id);
        if (task == null) {
            return null;
        }
        // 复用查表组
        params.put("mappingId", task.getId());
        return tableGroupService.search(params);
    }

    @Override
    public Object result(String id) {
        return taskService.result(id);
    }

    @Override
    public String refreshTables(String id) {
        ValidateSyncTask task = taskService.get(id);
        Assert.notNull(task, "The task id is invalid.");
        task.setSourceTable(updateConnectorTables(task, ConnectorInstanceUtil.SOURCE_SUFFIX));
        task.setTargetTable(updateConnectorTables(task, ConnectorInstanceUtil.TARGET_SUFFIX));
        taskService.edit(task);
        return id;
    }

    @Override
    public String refreshFields(String id) {
        TableGroup tableGroup = profileComponent.getTableGroup(id);
        Assert.notNull(tableGroup, "Can not find tableGroup.");

        ValidateSyncTask task = taskService.get(tableGroup.getMappingId());
        Assert.notNull(task, "The task id is invalid.");
        Table sourceTable = tableGroup.getSourceTable();
        Table targetTable = tableGroup.getTargetTable();
        List<String> sourceTablePks = sourceTable.getColumn().stream().filter(Field::isPk).map(Field::getName).collect(Collectors.toList());
        List<String> targetTablePks = targetTable.getColumn().stream().filter(Field::isPk).map(Field::getName).collect(Collectors.toList());
        validateSyncTableGroupChecker.updateTableColumn(task, ConnectorInstanceUtil.SOURCE_SUFFIX, StringUtil.join(sourceTablePks, ","), sourceTable);
        validateSyncTableGroupChecker.updateTableColumn(task, ConnectorInstanceUtil.TARGET_SUFFIX, StringUtil.join(targetTablePks, ","), targetTable);
        taskService.edit(task);
        return id;
    }

    @Override
    public String addTableGroup(Map<String, String> params) {
        String taskId = params.get("taskId");
        ValidateSyncTask task = taskService.get(taskId);
        // TODO 任务状态执行中
//        assertRunning(task);

        synchronized (LOCK) {
            // table1, table2
            String[] sourceTableArray = StringUtil.split(params.get("sourceTable"), StringUtil.VERTICAL_LINE);
            String[] targetTableArray = StringUtil.split(params.get("targetTable"), StringUtil.VERTICAL_LINE);
            int tableSize = sourceTableArray.length;
            Assert.isTrue(tableSize == targetTableArray.length, "数据源表和目标源表关系必须为一组");

            String id = null;
            List<String> list = new ArrayList<>();
            for (int i = 0; i < tableSize; i++) {
                params.put("sourceTable", sourceTableArray[i]);
                params.put("targetTable", targetTableArray[i]);
                TableGroup model = (TableGroup) validateSyncTableGroupChecker.checkAddConfigModel(params);
                log(LogType.TableGroupLog.INSERT, task, model);
                int tableGroupCount = profileComponent.getTableGroupCount(taskId);
                model.setIndex(tableGroupCount + 1);
                id = profileComponent.addTableGroup(model);
                list.add(id);
            }
            return 1 < tableSize ? String.valueOf(tableSize) : id;
        }
    }

    @Override
    public String editTableGroup(Map<String, String> params) {
        String tableGroupId = params.get(ConfigConstant.CONFIG_MODEL_ID);
        TableGroup tableGroup = profileComponent.getTableGroup(tableGroupId);
        Assert.notNull(tableGroup, "Can not find tableGroup.");
        ValidateSyncTask task = taskService.get(tableGroup.getMappingId());
        // TODO 任务状态执行中
//        assertRunning(task);

        TableGroup model = (TableGroup) validateSyncTableGroupChecker.checkEditConfigModel(params);
        log(LogType.TableGroupLog.UPDATE, task, tableGroup);
        profileComponent.editTableGroup(model);
        return tableGroupId;
    }

    @Override
    public String removeTableGroup(String taskId, String ids) {
        Assert.hasText(taskId, "Task id can not be null");
        Assert.hasText(ids, "TableGroup ids can not be null");
        ValidateSyncTask task = taskService.get(taskId);
        // TODO 任务状态执行中
//        assertRunning(task);

        // 批量删除表
        Stream.of(StringUtil.split(ids, ",")).parallel().forEach(id -> {
            TableGroup model = profileComponent.getTableGroup(id);
            log(LogType.TableGroupLog.DELETE, task, model);
            profileComponent.removeTableGroup(id);
        });
        // 重置排序
        resetTableGroupAllIndex(taskId);
        return taskId;
    }

    public List<Table> updateConnectorTables(ValidateSyncTask task, String suffix) {
        boolean isSource = StringUtil.equals(ConnectorInstanceUtil.SOURCE_SUFFIX, suffix);
        DefaultConnectorServiceContext context = ConnectorServiceContextUtil.buildConnectorServiceContext(task, isSource);
        String instanceId = ConnectorInstanceUtil.buildConnectorInstanceId(context.getMappingId(), context.getConnectorId(), context.getSuffix());
        ConnectorInstance connectorInstance = connectorFactory.connect(instanceId);
        List<Table> tables = connectorFactory.getTables(connectorInstance, context);
        // 按升序展示表
        Collections.sort(tables, Comparator.comparing(Table::getName));
        return tables;
    }

    private void resetTableGroupAllIndex(String taskId) {
        synchronized (LOCK) {
            List<TableGroup> list = profileComponent.getSortedTableGroupAll(taskId);
            int size = list.size();
            int i = size;
            while (i > 0) {
                TableGroup g = list.get(size - i);
                g.setIndex(i);
                profileComponent.editConfigModel(g);
                i--;
            }
        }
    }

    private ValidateSyncTaskVO convertTask2Vo(ValidateSyncTask task) {
        if (task == null) {
            return null;
        }

        Connector s = profileComponent.getConnector(task.getSourceConnectorId());
        Connector t = profileComponent.getConnector(task.getTargetConnectorId());
        ValidateSyncTaskVO vo = new ValidateSyncTaskVO(s, t);
        BeanUtils.copyProperties(task, vo);
        return vo;
    }

    private void checkTask(ValidateSyncTask task, Map<String, String> params) {
        if (StringUtil.isBlank(task.getId())) {
            task.setId(String.valueOf(snowflakeIdWorker.nextId()));
            task.setStatus(CommonTaskStatusEnum.READY.getCode());
            task.setType(CommonTaskTypeEnum.VALIDATE_SYNC.name());
        }
        long now = Instant.now().toEpochMilli();
        task.setCreateTime(null == task.getCreateTime() ? now : task.getCreateTime());
        task.setUpdateTime(now);
        task.setUpdateTime(System.currentTimeMillis());
        task.setName(params.get("name"));
        String trigger = params.get("trigger");
        String cron = params.get("cron");
        if (StringUtil.isNotBlank(trigger)) {
            CommonTaskTriggerEnum type = CommonTaskTriggerEnum.getType(trigger);
            Assert.notNull(type, "trigger is not valid");
            task.setTrigger(type.getCode());
        }
        if (StringUtil.isNotBlank(cron)) {
            task.setCron(cron);
        }
        task.setEnableSync(StringUtil.isNotBlank(params.get("enableSync")));
        task.setEnablerRowData(StringUtil.isNotBlank(params.get("enablerRowData")));
        task.setEnableIndex(StringUtil.isNotBlank(params.get("enableIndex")));
        task.setEnableTrigger(StringUtil.isNotBlank(params.get("enableTrigger")));
        task.setEnableFunction(StringUtil.isNotBlank(params.get("enableFunction")));
    }

    private void log(LogType log, ValidateSyncTask task, TableGroup tableGroup) {
        if (null != task) {
            // 新增订正校验任务知识库(执行一次)映射关系:[My_User] >> [My_User_Target]
            String name = task.getName();
            CommonTaskTriggerEnum type = CommonTaskTriggerEnum.getType(task.getTrigger());
            String s = tableGroup.getSourceTable().getName();
            String t = tableGroup.getTargetTable().getName();
            logService.log(log, "%s订正校验任务%s(%s)%s:[%s] >> [%s]", log.getMessage(), name, type.getMessage(), log.getName(), s, t);
        }
    }
}
