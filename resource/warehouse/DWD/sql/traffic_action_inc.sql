--建表语句
DROP TABLE IF EXISTS dwd_traffic_action_inc;
CREATE EXTERNAL TABLE dwd_traffic_action_inc
(
    `province_id`      STRING COMMENT '省份id',
    `brand`            STRING COMMENT '手机品牌',
    `channel`          STRING COMMENT '渠道',
    `is_new`           STRING COMMENT '是否首次启动',
    `model`            STRING COMMENT '手机型号',
    `mid_id`           STRING COMMENT '设备id',
    `operate_system`   STRING COMMENT '操作系统',
    `user_id`          STRING COMMENT '会员id',
    `version_code`     STRING COMMENT 'app版本号',
    `during_time`      BIGINT COMMENT '持续时间毫秒',
    `page_item`        STRING COMMENT '目标id ',
    `page_item_type`   STRING COMMENT '目标类型',
    `last_page_id`     STRING COMMENT '上页类型',
    `page_id`          STRING COMMENT '页面id ',
    `source_type`      STRING COMMENT '来源类型',
    `action_id`        STRING COMMENT '动作id',
    `action_item`      STRING COMMENT '目标id ',
    `action_item_type` STRING COMMENT '目标类型',
    `date_id`          STRING COMMENT '日期id',
    `action_time`      STRING COMMENT '动作发生时间'
) COMMENT '动作日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_action_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_action_inc partition (dt = '2020-06-14')
select province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       during_time,
       page_item,
       page_item_type,
       last_page_id,
       page_id,
       source_type,
       action_id,
       action_item,
       action_item_type,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') action_time
from (
         select common.ar        area_code,
                common.ba        brand,
                common.ch        channel,
                common.is_new,
                common.md        model,
                common.mid       mid_id,
                common.os        operate_system,
                common.uid       user_id,
                common.vc        version_code,
                page.during_time,
                page.item        page_item,
                page.item_type   page_item_type,
                page.last_page_id,
                page.page_id,
                page.source_type,
                action.action_id,
                action.item      action_item,
                action.item_type action_item_type,
                action.ts
         from ods_log_inc lateral view explode(actions) tmp as action
         where dt = '2020-06-14'
           and actions is not null
     ) log
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;
