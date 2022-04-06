--建表语句
DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份id',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备id',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员id',
    `version_code`   STRING COMMENT 'app版本号',
    `page_item`      STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页类型',
    `page_id`        STRING COMMENT '页面ID ',
    `source_type`    STRING COMMENT '来源类型',
    `date_id`        STRING COMMENT '日期id',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话id',
    `during_time`    BIGINT COMMENT '持续时间毫秒'
) COMMENT '页面日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_page_view_inc partition (dt = '2020-06-14')
select province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       page_item,
       page_item_type,
       last_page_id,
       page_id,
       source_type,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')                                        date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')                               view_time,
       concat(mid_id, '-', last_value(session_start_point, true) over (partition by mid_id order by ts)) session_id,
       during_time
from (
         select common.ar                               area_code,
                common.ba                               brand,
                common.ch                               channel,
                common.is_new                           is_new,
                common.md                               model,
                common.mid                              mid_id,
                common.os                               operate_system,
                common.uid                              user_id,
                common.vc                               version_code,
                page.during_time,
                page.item                               page_item,
                page.item_type                          page_item_type,
                page.last_page_id,
                page.page_id,
                page.source_type,
                ts,
                if(page.last_page_id is null, ts, null) session_start_point
         from ods_log_inc
         where dt = '2020-06-14'
           and page is not null
     ) log
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;
