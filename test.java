package com.ikscrm.scrm.intelligencetag.trigger;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.ikscrm.scrm.intelligencetag.constant.OrderStatus;
import com.ikscrm.scrm.intelligencetag.dao.*;
import com.ikscrm.scrm.intelligencetag.dao.mysql.*;
import com.ikscrm.scrm.intelligencetag.manager.TriggerCursorManager;
import com.ikscrm.scrm.intelligencetag.pojo.dto.*;
import com.ikscrm.scrm.intelligencetag.pojo.mongodb.BasicAttributesDO;
import com.ikscrm.scrm.intelligencetag.pojo.mongodb.OrderDO;
import com.ikscrm.scrm.intelligencetag.pojo.resp.TriggerDataResp;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author xz
 * @date 2022/12/10 14:33
 * mysql触发器
 */
@Component
@Slf4j
public class Trigger {

    @Autowired
    private TriggerCursorMapper triggerCursorMapper;

    @Autowired
    private CustomerRelationMapper customerRelationMapper;

    @Autowired
    private GroupChatMemberMapper groupChatMemberMapper;

    @Autowired
    private KpiEmployeeHistoryMapper kpiEmployeeHistoryMapper;

    @Autowired
    private CustomerMobileMapper customerMobileMapper;

    @Autowired
    private CustomerMapper customerMapper;

    @Autowired
    private CustomerBindAccountRelationMapper customerBindAccountRelationMapper;

    @Autowired
    private TriggerCursorManager triggerCursorManager;

    @Autowired
    private MongoTemplate mongoTemplate;

    // 订单中心mongodb地址
    @Value("${order.center.mongodb.uri}")
    private String orderCenterMongodb;

    // 场景code
    @Value("${customer-auto-tag.scenario-code}")
    private String autoScenarioCode;

    @Value("${tag-rule-engine.base-url}")
    private String baseUrl;
    // 推送变更数据URL
    private final static String SEND_URL = "/tag-rule-engine/v1/change-data/batch-push";
    //默认时间
    private final static Long EXCLUSION_DEFAULT_DATE = 253370736000000L;

    @Autowired
    private RestTemplate restTemplate;

    private MongoClient mongoClient;

    //主表名称
    private static final String MAIN_TABLE_NAME = "t_customer_relation";
    //发送批量大小
    private static final Integer BATCH_SIZE = 5000;
    //缓存时间,定期去数据库查询上次的时间,并且把最新的记录刷新到数据库,默认为五分钟
    private static final Integer CACHE_TIME = 1000 * 60 * 5;
    //增量休眠时间,单位为秒
    private static final Integer SLEEP_TIME = 5;

    // 需要监听的表
    private static final String[] tables = {"t_customer_relation", "t_kpi_employee_history", "t_customer_mobile", "t_group_chat_member", "t_customer", "t_customer_bind_account_relation", "t_data_order"};

    @PostConstruct
    private void init() {
        // 初始化mongodb
        mongoClient = MongoClients.create(orderCenterMongodb);
        //初始化监听
        new Thread(this::run).start();
    }

    /**
     * 触发器整体执行逻辑:
     * 1.首先判断本次是否需要全量清洗,首次全量,通过查询t_trigger_cursor的主表是否有记录来判断,没有记录就全量
     * 2.假如为全量清洗,首先基于当前时刻打印每个表的最大的update_time存在t_trigger_cursor表里,用于增量的开始时间
     * 3.主表通过id偏移拉取数据进行全量清洗,维表啥也不需要做
     * 4.批量发送触发id到标签引擎
     * 5.开启所有表增量,每次变更就更新t_trigger_cursor时间戳,主表直接发送到标签引擎,维表则需要通过维表id找到主表id,然后发送到标签引擎
     */

    private void run() {
        //1.首先判断本次是否需要全量清洗,首次全量,通过查询t_trigger_cursor的主表是否有记录来判断,没有记录就全量
        if (isFirstRun(MAIN_TABLE_NAME)) {
            //2.假如为全量清洗,首先基于当前时刻打印每个表的最大的update_time存在t_trigger_cursor表里,用于增量的开始时间
            MaxMinTimeDTO maxMinTimeDTO = customerRelationMapper.queryMaxMinId();
            if (maxMinTimeDTO != null) {
                snapshotAllTableTime(maxMinTimeDTO.getLastTime());
                //3.主表通过id偏移拉取数据进行全量清洗,维表啥也不需要做
                fullClean(maxMinTimeDTO);
            }
        }
        //5.开启所有表增量,每次变更就更新t_trigger_cursor时间戳,主表直接发送到标签引擎,维表则需要通过维表id找到主表id,然后发送到标签引擎
        log.info("开启增量清洗");
        // 开启主表监听
        new Thread(this::mainTableTrigger, "mainTableTrigger-Thread").start();
        // 开启t_customer_kpi_employee表监听
        new Thread(this::customerKpiEmployeeTableTrigger, "customerKpiEmployeeTableTrigger-Thread").start();
        // 开启t_customer_mobile表监听
        new Thread(this::customerMobileTableTrigger, "customerMobileTableTrigger-Thread").start();
        // 开启t_group_chat_member表监听
        new Thread(this::groupChatMemberTableTrigger, "groupChatMemberTableTrigger-Thread").start();
        // 开启t_customer表监听
        new Thread(this::customerTableTrigger, "customerTableTrigger-Thread").start();
        // 开启监听t_customer_bind_account_relation表监听
        new Thread(this::customerBindAccountRelationTrigger, "customerBindAccountRelationTrigger-Thread").start();
        // 开启监听t_data_order
        new Thread(this::dataOrderTrigger, "dataOrderTrigger-Thread").start();
    }

    /**
     * 主表(t_customer_relation)监听
     */
    private void mainTableTrigger() {
        String tableName = "t_customer_relation";
        // 进来先查询数据库
        String lastTime = triggerCursorMapper.queryLastTime(tableName);
        // 如果没数据,则直接从当前日期开始清洗
        if (!StringUtils.hasText(lastTime)) {
            lastTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
            // 第一次清洗,可能是需要进行全量清洗
            MaxMinTimeDTO maxMinTimeDTO = customerRelationMapper.queryMaxMinId();
            fullClean(maxMinTimeDTO);
        }
        long start = System.currentTimeMillis();
        int startNum = 0;
        while (true) {
            try {
                QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<>();
                queryWrapper.ge("update_time", lastTime);
                // 加上小于当前时间来防止异常时间戳导致的数据缺失
                queryWrapper.lt("update_time", DateUtil.now());
                queryWrapper.orderByAsc("update_time");
                String limitSql = String.format("limit %d,%d", startNum, BATCH_SIZE);
                queryWrapper.last(limitSql);
                List<CustomerRelationDO> cleanDTOS = customerRelationMapper.selectList(queryWrapper);
                if (!CollectionUtils.isEmpty(cleanDTOS)) {
                    sendAndAssembling(cleanDTOS);
                    CustomerRelationDO customerRelationDO = cleanDTOS.get(cleanDTOS.size() - 1);
                    LocalDateTime updateTime = customerRelationDO.getUpdateTime();
                    if (cleanDTOS.size() < BATCH_SIZE) {
                        // 一次取完了
                        startNum = 0;
                        String nowTime = updateTime.format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                        if (lastTime.equals(nowTime)) {
                            // 如果取完了,而且最大时间还是没有变,则手动加一秒
                            lastTime = DateUtil.offsetSecond(DateUtil.parse(lastTime), 1).toString();
                            log.info("取数完成,且最大时间还是没有变,手动增加lastTime 到:{}", lastTime);
                        } else {
                            lastTime = nowTime;
                            log.info("取数完成,更改最大时间lastTime为:{}", lastTime);
                        }
                    } else {
                        String nowTime = updateTime.format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                        if (lastTime.equals(nowTime)) {
                            // 一次没取完
                            startNum = startNum + BATCH_SIZE;
                        } else {
                            // 假如时间变了,则从新的时间开始,防止深度分页
                            startNum = 0;
                            lastTime = nowTime;
                        }
                        log.info("一次没取完,startNum:{},lastTime:{}", startNum, lastTime);
                    }
                } else {
                    // 取不到数据,则休眠一下
                    TimeUnit.SECONDS.sleep(SLEEP_TIME);
                }
                //刷新缓存时间,如果超过缓存时间,拿当前时间跟数据库的时间做一下对比,因为存在需要从指定时间开始拉取,或者重新开始全量清洗
                long now = System.currentTimeMillis();
                if (now - start > CACHE_TIME) {
                    start = now;
                    TriggerCursorDO triggerCursorDO = triggerCursorMapper.queryOne(tableName);
                    if (triggerCursorDO == null) {
                        // 假如数据被删了,可能是需要进行全量清洗
                        MaxMinTimeDTO maxMinTimeDTO = customerRelationMapper.queryMaxMinId();
                        if (maxMinTimeDTO != null) {
                            log.warn("{}表记录被人删除,开始全量清洗", tableName);
                            fullClean(maxMinTimeDTO);
                            lastTime = maxMinTimeDTO.getLastTime();
                            TriggerCursorDO triggerCursor = new TriggerCursorDO();
                            triggerCursor.setTableName(tableName);
                            triggerCursor.setLastTime(lastTime);
                            triggerCursorMapper.insert(triggerCursor);
                        }
                    } else {
                        // 如果被人修改了指定清洗状态,则开始从指定的时间开始清洗
                        if (triggerCursorDO.getIsSpecifyTime()) {
                            log.info("{}表时间被人修改,希望从指定的时间开始清洗:{}", tableName, triggerCursorDO.getLastTime());
                            lastTime = triggerCursorDO.getLastTime();
                            triggerCursorMapper.updateLastTime(new UpdateLastDTO(tableName, lastTime));
                        } else {
                            // 否则,落库存档
                            log.info("{}表缓存过期落库,落库时间为:{}", tableName, lastTime);
                            triggerCursorMapper.updateLastTime(new UpdateLastDTO(tableName, lastTime));
                        }
                    }
                }
            } catch (Exception ex) {
                log.error("{}:增量触发报错", tableName, ex);
            }
        }
    }

    /**
     * t_customer_mobile 监听
     * 顺序: t_customer_mobile -> t_customer_relation
     */
    private void customerMobileTableTrigger() {
        String tableName = "t_customer_mobile";
        // 进来先查询数据库
        String lastTime = triggerCursorMapper.queryLastTime(tableName);
        // 如果没数据,则直接从当前日期开始清洗
        if (!StringUtils.hasText(lastTime)) {
            lastTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
        }
        while (true) {
            try {
                List<SimpleDTO> cleanDTOS = customerMobileMapper.incQuery(lastTime);
                if (!CollectionUtils.isEmpty(cleanDTOS)) {
                    try {
                        if (cleanDTOS.size() <= 1000) {
                            List<Long> ids = cleanDTOS.stream().map(SimpleDTO::getId).distinct().collect(Collectors.toList());
                            QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<>();
                            queryWrapper.in("id", ids);
                            List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
                            sendAndAssembling(list);
                        } else {
                            List<List<SimpleDTO>> splitList = ListUtil.split(cleanDTOS, 1000);
                            for (List<SimpleDTO> simpleDTO : splitList) {
                                List<Long> ids = simpleDTO.stream().map(SimpleDTO::getId).distinct().collect(Collectors.toList());
                                QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<>();
                                queryWrapper.in("id", ids);
                                List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
                                sendAndAssembling(list);
                            }
                        }
                    } catch (Exception ex) {
                        log.error("{}:组装数据报错", tableName, ex);
                    }
                    Optional<SimpleDTO> max = cleanDTOS.stream().max(Comparator.comparing(SimpleDTO::getUpdateTime));
                    if (max.isPresent()) {
                        LocalDateTime updateTime = max.get().getUpdateTime();
                        lastTime = updateTime.format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                        saveTimeStamp(tableName, lastTime);
                    }
                } else {
                    // 取不到数据,则休眠一下
                    TimeUnit.SECONDS.sleep(SLEEP_TIME);
                }
            } catch (Exception ex) {
                log.error("{}:增量触发报错", tableName, ex);
            }
        }
    }

    /**
     * t_customer 监听
     * 顺序: t_customer -> t_customer_relation
     */
    private void customerTableTrigger() {
        String tableName = "t_customer";
        // 进来先查询数据库
        String lastTime = triggerCursorMapper.queryLastTime(tableName);
        // 如果没数据,则直接从当前日期开始清洗
        if (!StringUtils.hasText(lastTime)) {
            lastTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
        }
        while (true) {
            try {
                List<SimpleCustomerDTO> cleanDTOS = customerMapper.incQuery(lastTime);
                if (!CollectionUtils.isEmpty(cleanDTOS)) {
                    try {

                        // 为了能走索引,只能按照租户id先分组
                        List<CustomerRelationDO> sendList = new ArrayList<>();
                        cleanDTOS.stream()
                                .collect(Collectors.groupingBy(SimpleCustomerDTO::getTenantId))
                                .forEach((tenantId, simpleCustomerDTOs) -> {
                                    // 此处可能存在超出in的限制,只能分页处理了
                                    if (simpleCustomerDTOs.size() <= 1000) {
                                        List<String> customerIds = simpleCustomerDTOs.stream().map(SimpleCustomerDTO::getCustomerId).collect(Collectors.toList());
                                        QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<>();
                                        queryWrapper.eq("tenant_id", tenantId)
                                                .in("wecom_external_user_id", customerIds);
                                        List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
                                        if (!CollectionUtils.isEmpty(list)) {
                                            sendList.addAll(list);
                                        }
                                    } else {
                                        // 切分查询
                                        List<List<SimpleCustomerDTO>> splitList = ListUtil.split(simpleCustomerDTOs, 1000);
                                        for (List<SimpleCustomerDTO> simpleCustomerDTOS : splitList) {
                                            List<String> customerIds = simpleCustomerDTOS.stream().map(SimpleCustomerDTO::getCustomerId).collect(Collectors.toList());
                                            QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<CustomerRelationDO>();
                                            queryWrapper.eq("tenant_id", tenantId)
                                                    .in("wecom_external_user_id", customerIds);
                                            List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
                                            if (!CollectionUtils.isEmpty(list)) {
                                                sendList.addAll(list);
                                            }
                                        }
                                    }
                                });
                        sendAndAssembling(sendList);
                    } catch (Exception ex) {
                        log.error("{}:组装数据报错", tableName, ex);
                    }
                    Optional<SimpleCustomerDTO> max = cleanDTOS.stream().max(Comparator.comparing(SimpleCustomerDTO::getUpdateTime));
                    if (max.isPresent()) {
                        LocalDateTime updateTime = max.get().getUpdateTime();
                        lastTime = updateTime.format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                        saveTimeStamp(tableName, lastTime);
                    }
                } else {
                    // 查询不到数据,则休眠一下
                    TimeUnit.SECONDS.sleep(SLEEP_TIME);
                }
            } catch (Exception ex) {
                log.error("{}:增量触发报错", tableName, ex);
            }
        }
    }

    /**
     * t_group_chat_member 监听
     * 顺序: t_group_chat_member -> t_customer_relation
     */
    private void groupChatMemberTableTrigger() {
        String tableName = "t_group_chat_member";
        // 进来先查询数据库
        String lastTime = triggerCursorMapper.queryLastTime(tableName);
        // 如果没数据,则直接从当前日期开始清洗
        if (!StringUtils.hasText(lastTime)) {
            lastTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
        }
        while (true) {
            try {
                List<SimpleCustomerDTO> cleanDTOS = groupChatMemberMapper.incQuery(lastTime);
                if (!CollectionUtils.isEmpty(cleanDTOS)) {
                    // 为了能走索引,只能按照租户id先分组
                    List<CustomerRelationDO> sendList = new ArrayList<>();
                    cleanDTOS.stream()
                            .collect(Collectors.groupingBy(SimpleCustomerDTO::getTenantId))
                            .forEach((tenantId, simpleCustomerDTOs) -> {
                                // 此处可能存在超出in的限制,只能分页处理了
                                log.info("租户 : {}数据为 :{}", tenantId, simpleCustomerDTOs.size());
                                if (simpleCustomerDTOs.size() <= 1000) {
                                    List<String> customerIds = simpleCustomerDTOs.stream().map(SimpleCustomerDTO::getCustomerId).collect(Collectors.toList());
                                    QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<>();
                                    queryWrapper.eq("tenant_id", tenantId)
                                            .in("wecom_external_user_id", customerIds);
                                    List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
                                    if (!CollectionUtils.isEmpty(list)) {
                                        sendList.addAll(list);
                                    }
                                } else {
                                    // 切分查询
                                    List<List<SimpleCustomerDTO>> splitList = ListUtil.split(simpleCustomerDTOs, 1000);
                                    for (List<SimpleCustomerDTO> simpleCustomerDTOS : splitList) {
                                        List<String> customerIds = simpleCustomerDTOS.stream().map(SimpleCustomerDTO::getCustomerId).collect(Collectors.toList());
                                        QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<CustomerRelationDO>();
                                        queryWrapper.eq("tenant_id", tenantId)
                                                .in("wecom_external_user_id", customerIds);
                                        List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
                                        if (!CollectionUtils.isEmpty(list)) {
                                            sendList.addAll(list);
                                        }
                                    }
                                }
                            });
                    Optional<SimpleCustomerDTO> max = cleanDTOS.stream().max(Comparator.comparing(SimpleCustomerDTO::getUpdateTime));
                    if (max.isPresent()) {
                        LocalDateTime updateTime = max.get().getUpdateTime();
                        lastTime = updateTime.format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                        saveTimeStamp(tableName, lastTime);
                    }
                    sendAndAssembling(sendList);
                } else {
                    // 查询不到数据,则休眠一下
                    TimeUnit.SECONDS.sleep(SLEEP_TIME);
                }
            } catch (Exception ex) {
                log.error("{}:增量触发报错", tableName, ex);
            }
        }
    }


    /**
     * t_kpi_employee_history 监听
     * 顺序: t_kpi_employee_history -> t_customer_relation
     */
    private void customerKpiEmployeeTableTrigger() {
        String tableName = "t_kpi_employee_history";
        // 进来先查询数据库
        String lastTime = triggerCursorMapper.queryLastTime(tableName);
        // 如果没数据,则直接从当前日期开始清洗
        if (!StringUtils.hasText(lastTime)) {
            lastTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
        }
        while (true) {
            try {
                List<SimpleCustomerDTO> cleanDTOS = kpiEmployeeHistoryMapper.incQuery(lastTime);
                if (!CollectionUtils.isEmpty(cleanDTOS)) {
                    // 为了能走索引,只能按照租户id先分组
                    try {

                        List<CustomerRelationDO> sendList = new ArrayList<>();
                        cleanDTOS.stream()
                                .collect(Collectors.groupingBy(SimpleCustomerDTO::getTenantId))
                                .forEach((tenantId, simpleCustomerDTOs) -> {
                                    // 此处可能存在超出in的限制,只能分页处理了
                                    if (simpleCustomerDTOs.size() <= 1000) {
                                        List<String> customerIds = simpleCustomerDTOs.stream().map(SimpleCustomerDTO::getCustomerId).collect(Collectors.toList());
                                        QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<CustomerRelationDO>();
                                        queryWrapper.eq("tenant_id", tenantId)
                                                .in("wecom_external_user_id", customerIds);
                                        List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
                                        if (!CollectionUtils.isEmpty(list)) {
                                            sendList.addAll(list);
                                        }
                                    } else {
                                        // 切分查询
                                        List<List<SimpleCustomerDTO>> splitList = ListUtil.split(simpleCustomerDTOs, 1000);
                                        for (List<SimpleCustomerDTO> simpleCustomerDTOS : splitList) {
                                            List<String> customerIds = simpleCustomerDTOS.stream().map(SimpleCustomerDTO::getCustomerId).collect(Collectors.toList());
                                            QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<CustomerRelationDO>();
                                            queryWrapper.eq("tenant_id", tenantId)
                                                    .in("wecom_external_user_id", customerIds);
                                            List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
                                            if (!CollectionUtils.isEmpty(list)) {
                                                sendList.addAll(list);
                                            }
                                        }
                                    }
                                });
                        sendAndAssembling(sendList);
                    } catch (Exception ex) {
                        log.error("{}:组装数据报错", tableName, ex);
                    }
                    Optional<SimpleCustomerDTO> max = cleanDTOS.stream().max(Comparator.comparing(SimpleCustomerDTO::getUpdateTime));
                    if (max.isPresent()) {
                        LocalDateTime updateTime = max.get().getUpdateTime();
                        lastTime = updateTime.format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                        saveTimeStamp(tableName, lastTime);
                    }
                } else {
                    // 查不到数据,则休眠一下
                    TimeUnit.SECONDS.sleep(SLEEP_TIME);
                }
            } catch (Exception ex) {
                log.error("{}:增量触发报错", tableName, ex);
            }
        }
    }

    /**
     * t_customer_bind_account_relation 监听
     * 顺序: t_customer_bind_account_relation -> t_customer_relation
     */
    private void customerBindAccountRelationTrigger() {
        String tableName = "t_customer_bind_account_relation";
        // 进来先查询数据库
        String lastTime = triggerCursorMapper.queryLastTime(tableName);
        // 如果没数据,则直接从当前日期开始清洗
        if (!StringUtils.hasText(lastTime)) {
            lastTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
        }
        long start = System.currentTimeMillis();
        while (true) {
            try {
                List<SimpleCustomerDTO> cleanDTOS = customerBindAccountRelationMapper.incQuery(lastTime);
                if (!CollectionUtils.isEmpty(cleanDTOS)) {
                    // 为了能走索引,只能按照租户id先分组
                    try {

                        List<CustomerRelationDO> sendList = new ArrayList<>();
                        cleanDTOS.stream()
                                .collect(Collectors.groupingBy(SimpleCustomerDTO::getTenantId))
                                .forEach((tenantId, simpleCustomerDTOs) -> {
                                    // 此处可能存在超出in的限制,只能分页处理了
                                    if (simpleCustomerDTOs.size() <= 1000) {
                                        List<String> customerIds = simpleCustomerDTOs.stream().map(SimpleCustomerDTO::getCustomerId).collect(Collectors.toList());
                                        QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<CustomerRelationDO>();
                                        queryWrapper.eq("tenant_id", tenantId)
                                                .in("wecom_external_user_id", customerIds);
                                        List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
                                        if (!CollectionUtils.isEmpty(list)) {
                                            sendList.addAll(list);
                                        }
                                    } else {
                                        // 切分查询
                                        List<List<SimpleCustomerDTO>> splitList = ListUtil.split(simpleCustomerDTOs, 1000);
                                        for (List<SimpleCustomerDTO> simpleCustomerDTOS : splitList) {
                                            List<String> customerIds = simpleCustomerDTOS.stream().map(SimpleCustomerDTO::getCustomerId).collect(Collectors.toList());
                                            QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<CustomerRelationDO>();
                                            queryWrapper.eq("tenant_id", tenantId)
                                                    .in("wecom_external_user_id", customerIds);
                                            List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
                                            if (!CollectionUtils.isEmpty(list)) {
                                                sendList.addAll(list);
                                            }
                                        }
                                    }
                                });
                        sendAndAssembling(sendList);
                    } catch (Exception ex) {
                        log.error("{}:组装数据报错", tableName, ex);
                    }
                    Optional<SimpleCustomerDTO> max = cleanDTOS.stream().max(Comparator.comparing(SimpleCustomerDTO::getUpdateTime));
                    if (max.isPresent()) {
                        LocalDateTime updateTime = max.get().getUpdateTime();
                        lastTime = updateTime.format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                        saveTimeStamp(tableName, lastTime);
                    }
                } else {
                    TimeUnit.SECONDS.sleep(SLEEP_TIME);
                }
            } catch (Exception ex) {
                log.error("{}:增量触发报错", tableName, ex);
            }
        }
    }


    /**
     * t_data_order表监听
     * 顺序: t_data_order -> t_employee_customer_tag_relation -> t_customer_relation
     */
    private void dataOrderTrigger() {
        String tableName = "t_data_order";
        // 进来先查询数据库
        String lastTime = triggerCursorMapper.queryLastTime(tableName);
        // 如果没数据,则直接从当前日期开始清洗
        if (!StringUtils.hasText(lastTime)) {
            lastTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
        }
        while (true) {
            try {
                MongoCollection<Document> collection = mongoClient.getDatabase("order-center").getCollection(tableName);
                Bson projection = Projections.fields(Projections.include("buyer_nick_open_id", "update_time", "tenant_id"), Projections.excludeId());
                List<DataOrderDTO> documents = new ArrayList<>();
                collection
                        .find(Filters.gt("update_time", lastTime)).projection(projection)
                        .sort(Sorts.ascending("update_time"))
                        .forEach(document -> {
                            documents.add(new DataOrderDTO(document.getInteger("tenant_id"), document.getString("update_time"), document.getString("buyer_nick_open_id")));
                        });
                if (!CollectionUtils.isEmpty(documents)) {
                    try {

                        log.info("{}表触发更新,条数为:{}", tableName, documents.size());
                        lastTime = documents.get(documents.size() - 1).getUpdateTime();
                        documents.stream()
                                .collect(Collectors.groupingBy(DataOrderDTO::getTenantId))
                                .forEach((tenantId, list) -> {
                                    List<String> openIds = list.stream().map(DataOrderDTO::getBuyerNickOpenId).distinct().collect(Collectors.toList());
                                    QueryWrapper<CustomerBindAccountRelationDO> queryWrapper = new QueryWrapper<>();
                                    queryWrapper.eq("tenant_id", tenantId)
                                            .in("buyer_nick_open_id", openIds)
                                            .eq("bind_status", 1);
                                    List<CustomerBindAccountRelationDO> customerBindAccountRelationDOS = customerBindAccountRelationMapper.selectList(queryWrapper);
                                    if (!CollectionUtils.isEmpty(customerBindAccountRelationDOS)) {
                                        List<String> customerIds = customerBindAccountRelationDOS.stream().map(CustomerBindAccountRelationDO::getExternalUserId).collect(Collectors.toList());
                                        QueryWrapper<CustomerRelationDO> query = new QueryWrapper<CustomerRelationDO>();
                                        query.eq("tenant_id", tenantId)
                                                .in("wecom_external_user_id", customerIds);
                                        List<CustomerRelationDO> customerRelationList = customerRelationMapper.selectList(query);
                                        sendAndAssembling(customerRelationList);
                                    }
                                });
                    } catch (Exception ex) {
                        log.error("{}:组装数据报错", tableName, ex);
                    }
                    saveTimeStamp(tableName, lastTime);
                } else {
                    TimeUnit.SECONDS.sleep(SLEEP_TIME);
                }
            } catch (Exception ex) {
                log.error("{}:增量触发报错", tableName, ex);
            }
        }
    }
//    private void dataOrderTrigger() {
//        MongoDatabase mongoDatabase = mongoClient.getDatabase("order-center");
//        List<Bson> pipeline = Arrays.asList(
//                Aggregates.match(
//                        Filters.in("operationType",
//                                Arrays.asList("insert", "update"))
//                ), Aggregates.match(Filters.in("ns.coll", "t_data_order")));
//        ChangeStreamIterable<Document> changeStream = mongoDatabase.watch(pipeline)
//                .fullDocument(FullDocument.UPDATE_LOOKUP);
//        changeStream.forEach(event -> {
//            try {
//                Document fullDocument = event.getFullDocument();
//                log.info("mongodb监听变化 : {}", fullDocument);
//                if (fullDocument != null) {
//                    String openId = fullDocument.getString("buyer_nick_open_id");
//                    int tenantId = fullDocument.getInteger("tenant_id");
//                    QueryWrapper<CustomerBindAccountRelationDO> queryWrapper = new QueryWrapper<>();
//                    queryWrapper.eq("tenant_id", tenantId)
//                            .eq("buyer_nick_open_id", openId)
//                            .eq("bind_status", 1);
//                    List<CustomerBindAccountRelationDO> customerBindAccountRelationDOS = customerBindAccountRelationMapper.selectList(queryWrapper);
//                    if (!CollectionUtils.isEmpty(customerBindAccountRelationDOS)) {
//                        List<String> customerIds = customerBindAccountRelationDOS.stream().map(CustomerBindAccountRelationDO::getExternalUserId).collect(Collectors.toList());
//                        QueryWrapper<CustomerRelationDO> query = new QueryWrapper<CustomerRelationDO>();
//                        query.eq("tenant_id", tenantId)
//                                .in("wecom_external_user_id", customerIds);
//                        List<CustomerRelationDO> list = customerRelationMapper.selectList(query);
//                        sendAndAssembling(list);
//                    }
//                }
//            } catch (Exception ex) {
//                log.error("监听表报错:", ex);
//            }
//        });
//    }


    /**
     * @param lastTime 主表的最后更新时间,其实也是所有的维表的起始时间,暂时不考虑时间戳不规范的场景
     */
    private void snapshotAllTableTime(String lastTime) {
        // 先查询数据库里面的所有的数据
        Map<String, Long> map = triggerCursorManager.lambdaQuery()
                .select(TriggerCursorDO::getId, TriggerCursorDO::getTableName)
                .list()
                .stream()
                .collect(Collectors.toMap(TriggerCursorDO::getTableName, TriggerCursorDO::getId));
        List<TriggerCursorDO> triggerCursorDOList = Arrays.stream(tables)
                .map(table -> {
                    TriggerCursorDO triggerCursorDO = new TriggerCursorDO();
                    triggerCursorDO.setId(map.get(table));
                    triggerCursorDO.setLastTime(lastTime);
                    triggerCursorDO.setTableName(table);
                    return triggerCursorDO;
                }).collect(Collectors.toList());
        triggerCursorManager.saveOrUpdateBatch(triggerCursorDOList);
    }

    private void fullClean(MaxMinTimeDTO maxMinTimeDTO) {
        log.info("开启全量清洗");
        Long minId = maxMinTimeDTO.getMinId();
        Long maxId = maxMinTimeDTO.getMaxId();
        while (true) {
            // 批量查询id
            QueryWrapper<CustomerRelationDO> queryWrapper = new QueryWrapper<>();
            queryWrapper.ge("id", minId)
                    .le("id", maxId)
                    .orderByAsc("id")
                    .last("limit " + BATCH_SIZE);
            List<CustomerRelationDO> list = customerRelationMapper.selectList(queryWrapper);
            // 4.批量发送触发id到标签引擎
            sendAndAssembling(list);
            if (list.size() < BATCH_SIZE) {
                // 取完了
                break;
            }
            // 获取当前批次最大id
            OptionalLong max = list.stream().mapToLong(CustomerRelationDO::getId).max();
            if (max.isPresent()) {
                minId = max.getAsLong();
            } else {
                break;
            }
        }
        log.info("全量清洗完成");
    }

    /**
     * sendAndAssembling
     * 发送和拼装数据
     *
     * @param sendData
     */
    //todo 此处可以做一个时间窗口,减少多表同时变更导致的重复计算
    private void sendAndAssembling(List<CustomerRelationDO> sendData) {
        if (!CollectionUtils.isEmpty(sendData)) {
            // 按照租户分组
            sendData.stream()
                    .collect(Collectors.groupingBy(CustomerRelationDO::getTenantId))
                    .forEach((tenantId, list) -> {
                        // 拼装数据
                        List<BasicAttributesDO> fullList = findAssemblingData(list, tenantId);
                        List<String> ids = fullList.stream().map(BasicAttributesDO::getLdSrcId).collect(Collectors.toList());
                        long startSink = System.currentTimeMillis();
                        // 写入mongodb
                        sinkMongoDb(fullList, ids);
                        long endSink = System.currentTimeMillis();
                        log.info("写入mongodb耗时 : {}ms,写入批次大小:{} ", endSink - startSink, fullList.size());
                        // 发送到标签引擎
                        sendToTagRuleEngine(ids, tenantId);
                    });
        }
    }


    /**
     * sink到mongodb
     *
     * @param list
     */
    private synchronized void sinkMongoDb(List<BasicAttributesDO> list, List<String> ids) {
        if (!CollectionUtils.isEmpty(list)) {
            try {
                Query query = new Query().addCriteria(Criteria.where("_id").in(ids));
                mongoTemplate.findAllAndRemove(query, BasicAttributesDO.class);
                // 只新增状态对的数据
                list = list.stream().filter(c -> c.getIsDeleted() == null).distinct().collect(Collectors.toList());
                mongoTemplate.insertAll(list);
            } catch (Exception ex) {
                log.error("写入mongodb报错,写入数据:{}", JSONUtil.toJsonStr(list), ex);
            }

        }
    }

    /**
     * 发送触发id到标签引擎
     *
     * @param ids      主表id数据
     * @param tenantId 租户id
     */
    private void sendToTagRuleEngine(List<String> ids, Integer tenantId) {
        TriggerDataResp triggerDataResp = new TriggerDataResp(autoScenarioCode, tenantId.longValue(), ids);
        // 推送自动打标签
        CompletableFuture.runAsync(() -> {
            log.info("本次推送到标签引擎的数量为 : {},推送URL:{} ", ids.size(), baseUrl + SEND_URL);
            restTemplate.postForEntity(baseUrl + SEND_URL, triggerDataResp, String.class);
        });
    }

    /**
     * 查询组装数据
     *
     * @return
     */
    private List<BasicAttributesDO> findAssemblingData(List<CustomerRelationDO> customerData, Integer tenantId) {
        try {
            List<Long> mainIds = customerData.stream().map(CustomerRelationDO::getId).collect(Collectors.toList());
            List<String> wecomExternalUserIds = customerData
                    .stream()
                    .map(CustomerRelationDO::getWecomExternalUserId)
                    .distinct()
                    .collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(wecomExternalUserIds)) {
                CompletableFuture<List<KpiEmployeeHistoryDO>> kpiCompletableFuture = CompletableFuture.supplyAsync(() -> {
                    // 查询责任员工
                    QueryWrapper<KpiEmployeeHistoryDO> kpiQueryWrapper = new QueryWrapper<>();
                    kpiQueryWrapper.in("wecom_external_user_id", wecomExternalUserIds)
                            .eq("tenant_id", tenantId)
                            .eq("status", 1)
                            .eq("is_deleted", 0);
                    return kpiEmployeeHistoryMapper.selectList(kpiQueryWrapper);
                });
                CompletableFuture<List<GroupChatMemberDO>> groupCompletableFuture = CompletableFuture.supplyAsync(() -> {
                    //查询群数据
                    QueryWrapper<GroupChatMemberDO> groupQueryWrapper = new QueryWrapper<>();
                    groupQueryWrapper.in("member_user_id", wecomExternalUserIds)
                            .eq("tenant_id", tenantId)
                            .eq("is_deleted", 0);
                    return groupChatMemberMapper.selectList(groupQueryWrapper);
                });
                CompletableFuture<List<CustomerMobileDO>> mobileCompletableFuture = CompletableFuture.supplyAsync(() -> {
                    QueryWrapper<CustomerMobileDO> mobileQueryWrapper = new QueryWrapper<>();
                    mobileQueryWrapper.in("customer_relations_id", mainIds)
                            .eq("tenant_id", tenantId)
                            .eq("is_deleted", 0);
                    return customerMobileMapper.selectList(mobileQueryWrapper);
                });
                // 查询客户生日
                CompletableFuture<List<CustomerDO>> customerCompletableFuture = CompletableFuture.supplyAsync(() -> {
                    QueryWrapper<CustomerDO> customerQueryWrapper = new QueryWrapper<>();
                    customerQueryWrapper.in("wecom_external_user_id", wecomExternalUserIds)
                            .eq("tenant_id", tenantId)
                            .eq("is_deleted", 0);
                    return customerMapper.selectList(customerQueryWrapper);
                });
                // 查询绑定信息
                CompletableFuture<List<CustomerBindAccountRelationDO>> customerBindAccountFuture = CompletableFuture.supplyAsync(() -> {
                    QueryWrapper<CustomerBindAccountRelationDO> customerBindWrapper = new QueryWrapper<>();
                    customerBindWrapper.in("external_user_id", wecomExternalUserIds)
                            .eq("tenant_id", tenantId)
                            .eq("bind_status", 1)
                            .eq("is_deleted", 0);
                    return customerBindAccountRelationMapper.selectList(customerBindWrapper);
                });

                // 等待查询完毕
                CompletableFuture.allOf(kpiCompletableFuture, groupCompletableFuture, mobileCompletableFuture, customerCompletableFuture, customerBindAccountFuture).get();
                //获取结果
                List<KpiEmployeeHistoryDO> kpiData = kpiCompletableFuture.get();
                List<GroupChatMemberDO> chatData = groupCompletableFuture.get();
                List<CustomerMobileDO> mobileData = mobileCompletableFuture.get();
                List<CustomerDO> customerFutureData = customerCompletableFuture.get();
                List<CustomerBindAccountRelationDO> customerBindAccountRelationDOS = customerBindAccountFuture.get();
                // 通过绑定信息获取电商信息
                List<String> openIds = customerBindAccountRelationDOS.stream().map(CustomerBindAccountRelationDO::getBuyerNickOpenId).distinct().collect(Collectors.toList());
                // 获取电商信息
                List<ElectronicBusinessDTO> electronicBusinessInfo = getElectronicBusinessInfo(openIds, tenantId);

                // 转化成map,进行组装计算
                Map<String, String> kpiMap = kpiData.stream().collect(Collectors.toMap(KpiEmployeeHistoryDO::getWecomExternalUserId, KpiEmployeeHistoryDO::getUserId, (s1, s2) -> s2));
                Map<String, List<GroupChatMemberDO>> groupMap = chatData.stream().collect(Collectors.groupingBy(GroupChatMemberDO::getMemberUserId));
                Map<Long, List<CustomerMobileDO>> mobileMap = mobileData.stream().collect(Collectors.groupingBy(CustomerMobileDO::getCustomerRelationsId));
                Map<String, Date> customerMap = customerFutureData.stream().filter(c -> c != null && c.getBirthday() != null && c.getBirthday().getTime() != EXCLUSION_DEFAULT_DATE).collect(Collectors.toMap(CustomerDO::getWecomExternalUserId, CustomerDO::getBirthday, (s1, s2) -> s2));

                Map<String, List<ElectronicBusinessDTO>> electronicBusinessMap = electronicBusinessInfo.stream().collect(Collectors.groupingBy(ElectronicBusinessDTO::getOpenId));
                Map<String, List<CustomerBindAccountRelationDO>> bindMap = customerBindAccountRelationDOS.stream().collect(Collectors.groupingBy(CustomerBindAccountRelationDO::getExternalUserId));

                List<BasicAttributesDO> basicAttributesDOS = customerData
                        .stream()
                        .map(customer -> {
                            BasicAttributesDO basicAttributesDO = new BasicAttributesDO();
                            basicAttributesDO.setTenantId(tenantId);
                            basicAttributesDO.setXtCreateDate(new Date());
                            basicAttributesDO.setLdSrcId(customer.getId() + "");
                            basicAttributesDO.setWecomExternalUserId(customer.getWecomExternalUserId());
                            basicAttributesDO.setAddCustomerTime(customer.getAddCustomerTime());
                            basicAttributesDO.setWecomAddWay(customer.getWecomAddWay());
                            basicAttributesDO.setChannelId(customer.getChannelId());
                            basicAttributesDO.setUserId(customer.getUserId());
                            basicAttributesDO.setKpiEmployeeId(kpiMap.get(customer.getWecomExternalUserId()));
                            basicAttributesDO.setRemark(customer.getRemark());
                            basicAttributesDO.setCustomerBirthday(customerMap.get(customer.getWecomExternalUserId()));
                            if (mobileMap.containsKey(customer.getId())) {
                                basicAttributesDO.setMobile(1);
                            } else {
                                basicAttributesDO.setMobile(0);
                            }
                            basicAttributesDO.setGroupChatSize(0);
                            if (groupMap.containsKey(customer.getWecomExternalUserId())) {
                                List<GroupChatMemberDO> groupChatMemberDOS = groupMap.get(customer.getWecomExternalUserId());
                                List<String> groupIds = groupChatMemberDOS.stream().map(GroupChatMemberDO::getGroupId).collect(Collectors.toList());
                                if (!CollectionUtils.isEmpty(groupIds)) {
                                    basicAttributesDO.setGroupChatIdList(String.join("|", groupIds));
                                }
                                basicAttributesDO.setGroupChatSize(groupIds.size());
                            }
                            if (customer.getIsDeleted() || customer.getRelationRemoveType() != 0) {
                                basicAttributesDO.setIsDeleted(true);
                            }

                            List<CustomerBindAccountRelationDO> customerBindAccountRelationDOS1 = bindMap.get(customer.getWecomExternalUserId());
                            List<ElectronicBusinessDTO> electronicBusinessDTOS = new ArrayList<>();
                            if (!CollectionUtils.isEmpty(customerBindAccountRelationDOS1)) {
                                for (CustomerBindAccountRelationDO customerBindAccountRelationDO : customerBindAccountRelationDOS1) {
                                    if (!CollectionUtils.isEmpty(electronicBusinessMap.get(customerBindAccountRelationDO.getBuyerNickOpenId()))) {
                                        electronicBusinessDTOS.addAll(electronicBusinessMap.get(customerBindAccountRelationDO.getBuyerNickOpenId()));
                                    }
                                }
                            }
                            // 赋值电商属性
                            basicAttributesDO.setElectronicBusinessInfoList(electronicBusinessDTOS);
                            return basicAttributesDO;
                        })
                        .collect(Collectors.toList());
                return basicAttributesDOS;
            }
        } catch (Exception ex) {
            log.error("查询报错", ex);
        }
        return new ArrayList<>();
    }

    /**
     * 通过绑定id,查询电商信息
     */
    private List<ElectronicBusinessDTO> getElectronicBusinessInfo(List<String> openIds, Integer tenantId) {
        MongoDatabase mongoDatabase = mongoClient.getDatabase("order-center");
        MongoCollection<Document> orderColl = mongoDatabase.getCollection("t_data_order");

        Bson query = Filters.and(Filters.eq("tenant_id", tenantId),
                Filters.in("buyer_nick_open_id", openIds),
                Filters.nin("status", OrderStatus.TRADE_NO_PAY.getStatus(), OrderStatus.TRADE_CANCELED.getStatus())
        );

        FindIterable<Document> orderDOS = orderColl.find(query);
        List<OrderDO> orderRes = new ArrayList<>();
        orderDOS.forEach(c -> orderRes.add(BeanUtil.copyProperties(c, OrderDO.class)));

        List<ObjectId> orderIds = orderRes.stream().map(OrderDO::get_id).collect(Collectors.toList());

        Map<ObjectId, OrderDO> orderMap = orderRes.stream().collect(Collectors.toMap(OrderDO::get_id, OrderDO -> OrderDO, (s1, s2) -> s2));

        MongoCollection<Document> subOrder = mongoDatabase.getCollection("t_data_sub_order");
        Bson filter = Filters.and(Filters.eq("tenant_id", tenantId), Filters.in("order_id", orderIds));
        FindIterable<Document> subOrderDOS = subOrder.find(filter);
        List<Document> subOrderList = new ArrayList<>();
        subOrderDOS.forEach(subOrderList::add);
        List<ElectronicBusinessDTO> ebList = new ArrayList<>();
        for (Document subOrderDO : subOrderList) {
            ElectronicBusinessDTO electronicBusinessDTO = new ElectronicBusinessDTO();
            ObjectId orderId = subOrderDO.getObjectId("order_id");
            OrderDO order = orderMap.get(orderId);
            electronicBusinessDTO.setOpenId(order.getBuyerNickOpenId());
            electronicBusinessDTO.setShopId(order.getShopId());
            electronicBusinessDTO.setOrderCreateTime(order.getOrderCreateTime());
            electronicBusinessDTO.setStatus(order.getStatus());
            electronicBusinessDTO.setPayment(order.getPayment());
            electronicBusinessDTO.setSkuId(subOrderDO.getString("sku_id"));
            electronicBusinessDTO.setProductCode(subOrderDO.getString("product_code"));
            electronicBusinessDTO.setOrderId(subOrderDO.getObjectId("order_id").toHexString());
            ebList.add(electronicBusinessDTO);
        }
        return ebList;
    }

    /**
     * 是否首次跑任务
     *
     * @param tableName 表名
     * @return
     */
    private boolean isFirstRun(String tableName) {
        return triggerCursorMapper.isFirstRun(tableName) == 0;
    }


    /**
     * 存储表的时间戳
     *
     * @param tableName 表名
     * @param lastTime  最后的时间
     */
    private void saveTimeStamp(String tableName, String lastTime) {
        TriggerCursorDO triggerCursorDO = triggerCursorMapper.queryOne(tableName);
        if (triggerCursorDO == null) {
            log.warn("{}表记录被人删除,记录清洗时间为当前时间", tableName);
            lastTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
            TriggerCursorDO triggerCursor = new TriggerCursorDO();
            triggerCursor.setTableName(tableName);
            triggerCursor.setLastTime(lastTime);
            triggerCursorMapper.insert(triggerCursor);
        } else {
            // 落库存档
            log.info("{}表落库,落库时间为:{}", tableName, lastTime);
            triggerCursorMapper.updateLastTime(new UpdateLastDTO(tableName, lastTime));
        }
    }
}
