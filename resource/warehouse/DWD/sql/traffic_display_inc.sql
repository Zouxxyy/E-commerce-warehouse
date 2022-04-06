--建表语句
DROP TABLE IF EXISTS dwd_traffic_display_inc;
CREATE EXTERNAL TABLE dwd_traffic_display_inc
(
    `province_id`       STRING COMMENT '省份id',
    `brand`             STRING COMMENT '手机品牌',
    `channel`           STRING COMMENT '渠道',
    `is_new`            STRING COMMENT '是否首次启动',
    `model`             STRING COMMENT '手机型号',
    `mid_id`            STRING COMMENT '设备id',
    `operate_system`    STRING COMMENT '操作系统',
    `user_id`           STRING COMMENT '会员id',
    `version_code`      STRING COMMENT 'app版本号',
    `during_time`       BIGINT COMMENT 'app版本号',
    `page_item`         STRING COMMENT '目标id ',
    `page_item_type`    STRING COMMENT '目标类型',
    `last_page_id`      STRING COMMENT '上页类型',
    `page_id`           STRING COMMENT '页面ID ',
    `source_type`       STRING COMMENT '来源类型',
    `date_id`           STRING COMMENT '日期id',
    `display_time`      STRING COMMENT '曝光时间',
    `display_type`      STRING COMMENT '曝光类型',
    `display_item`      STRING COMMENT '曝光对象id ',
    `display_item_type` STRING COMMENT 'app版本号',
    `display_order`     BIGINT COMMENT '曝光顺序',
    `display_pos_id`    BIGINT COMMENT '曝光位置'
) COMMENT '曝光日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_display_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_display_inc partition (dt = '2020-06-14')
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
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') display_time,
       display_type,
       display_item,
       display_item_type,
       display_order,
       display_pos_id
from (
         select common.ar         area_code,
                common.ba         brand,
                common.ch         channel,
                common.is_new,
                common.md         model,
                common.mid        mid_id,
                common.os         operate_system,
                common.uid        user_id,
                common.vc         version_code,
                page.during_time,
                page.item         page_item,
                page.item_type    page_item_type,
                page.last_page_id,
                page.page_id,
                page.source_type,
                display.display_type,
                display.item      display_item,
                display.item_type display_item_type,
                display.`order`   display_order,
                display.pos_id    display_pos_id,
                ts
         from ods_log_inc lateral view explode(displays) tmp as display
         where dt = '2020-06-14'
           and displays is not null
     ) log
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;
