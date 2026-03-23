/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ValidateSyncService;
import org.dbsyncer.biz.vo.ValidateSyncTaskVO;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.sdk.model.CommonTask;
import org.dbsyncer.sdk.spi.TaskService;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Service
public class ValidateSyncServiceImpl implements ValidateSyncService {

    @Resource
    private TaskService taskService;

    @Resource
    private ProfileComponent profileComponent;

    @Override
    public ValidateSyncTaskVO get(String id) {
        CommonTask commonTask = taskService.get(id);
        return convertTask2Vo((ValidateSyncTask) commonTask);
    }

    @Override
    public String add(Map<String, String> params) {
        return taskService.add(params);
    }

    @Override
    public String edit(Map<String, String> params) {
        return taskService.edit(params);
    }

    @Override
    public String copy(String id) {
        return taskService.copy(id);
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
            if(task instanceof ValidateSyncTask){
                ValidateSyncTask t = (ValidateSyncTask) task;
                list.add(convertTask2Vo(t));
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
        Connector s = profileComponent.getConnector(task.getSourceConnectorId());
        Connector t = profileComponent.getConnector(task.getTargetConnectorId());
        ValidateSyncTaskVO vo = new ValidateSyncTaskVO(s, t);
        BeanUtils.copyProperties(task, vo);
        return vo;
    }
}
