/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ValidateSyncTaskVO;
import org.dbsyncer.common.model.Paging;

import java.util.Map;

public interface ValidateSyncService {

    /**
     * 获取任务
     *
     * @param id
     * @return
     */
    ValidateSyncTaskVO get(String id);

    /**
     * 添加任务
     *
     * @param params
     * @return
     */
    String add(Map<String, String> params);

    /**
     * 修改任务
     *
     * @param params
     * @return
     */
    String edit(Map<String, String> params);

    /**
     * 复制任务
     *
     * @param id
     * @return
     */
    String copy(String id);

    /**
     * 删除任务
     *
     * @param id
     * @return
     */
    String delete(String id);

    /**
     * 启动任务
     *
     * @param id
     * @return
     */
    String start(String id);

    /**
     * 停止任务
     *
     * @param id
     * @return
     */
    String stop(String id);

    /**
     * 搜索任务
     *
     * @param params
     * @return
     */
    Paging<ValidateSyncTaskVO> search(Map<String, String> params);

    /**
     * 获取结果
     *
     * @param id
     * @return
     */
    Object result(String id);
}
