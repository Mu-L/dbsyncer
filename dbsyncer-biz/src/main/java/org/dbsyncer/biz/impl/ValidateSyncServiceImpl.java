/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ValidateSyncService;
import org.dbsyncer.biz.vo.ValidateSyncTaskVO;
import org.dbsyncer.common.enums.CommonTaskStatusEnum;
import org.dbsyncer.common.enums.CommonTaskTriggerEnum;
import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
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
import java.util.List;
import java.util.Map;

@Service
public class ValidateSyncServiceImpl implements ValidateSyncService {

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private TaskService<ValidateSyncTask> taskService;

    @Resource
    private ProfileComponent profileComponent;

    @Override
    public ValidateSyncTaskVO get(String id) {
        return convertTask2Vo(taskService.get(id));
    }

    @Override
    public String add(Map<String, String> params) {
        String mappingId = params.get("mappingId");
        ValidateSyncTask task = new ValidateSyncTask();
        checkTask(task, params);
        // 默认检查行数据
        task.setEnablerRowData(true);
        // 关联同步任务
        if (StringUtil.isNotBlank(mappingId)) {
            Mapping mapping = profileComponent.getMapping(mappingId);
            Assert.notNull(mapping, "mapping is not exist");
            task.setMappingId(mappingId);
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
        return taskService.add(task);
    }

    @Override
    public String edit(Map<String, String> params) {
        ValidateSyncTask task = taskService.get(params.get("id"));
        if (task == null) {
            throw new IllegalArgumentException("Task not found");
        }
        checkTask(task, params);
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
        return taskService.add(newTask);
    }

    @Override
    public String delete(String id) {
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
    public Object result(String id) {
        return taskService.result(id);
    }

    private ValidateSyncTaskVO convertTask2Vo(ValidateSyncTask task) {
        if (task == null) {
            return null;
        }
        // 关联同步任务
        if (StringUtil.isNotBlank(task.getMappingId())) {
            Mapping mapping = profileComponent.getMapping(task.getMappingId());
            Connector s = profileComponent.getConnector(mapping.getSourceConnectorId());
            Connector t = profileComponent.getConnector(mapping.getTargetConnectorId());
            ValidateSyncTaskVO vo = new ValidateSyncTaskVO(s, t);
            BeanUtils.copyProperties(task, vo);
            vo.setSourceConnectorId(mapping.getSourceConnectorId());
            vo.setSourceDatabase(mapping.getSourceDatabase());
            vo.setSourceSchema(mapping.getSourceSchema());
            vo.setSourceTable(mapping.getSourceTable());
            vo.setTargetConnectorId(mapping.getTargetConnectorId());
            vo.setTargetDatabase(mapping.getTargetDatabase());
            vo.setTargetSchema(mapping.getTargetSchema());
            vo.setTargetTable(mapping.getTargetTable());
            return vo;
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

}
