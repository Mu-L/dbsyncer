/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ValidateSyncTaskVO;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.model.TableGroup;

import java.util.List;
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
     * 搜索表关系
     *
     * @param params
     * @return
     */
    Paging<TableGroup> searchTableGroup(Map<String, String> params);

    /**
     * 获取结果
     *
     * @param id
     * @return
     */
    Object result(String id);

    /**
     * 刷新表列表
     *
     * @param id
     * @return
     */
    String refreshTables(String id);

    /**
     * 刷新表字段
     *
     * @param id
     * @return
     */
    String refreshFields(String id);

    /**
     * 获取所有任务列表（用于下拉选择）
     *
     * @return 任务VO列表
     */
    List<ValidateSyncTaskVO> getAll();

    /**
     * 分页查询校验结果明细
     *
     * @param params 包含 taskId、pageNum、pageSize
     * @return 分页结果
     */
    Paging searchResult(Map<String, String> params);

    String addTableGroup(Map<String, String> params);

    String editTableGroup(Map<String, String> params);

    String removeTableGroup(String id, String ids);
}
