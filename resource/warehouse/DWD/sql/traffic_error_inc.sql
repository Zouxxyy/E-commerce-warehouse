--建表语句
DROP TABLE IF EXISTS dwd_traffic_error_inc;
CREATE EXTERNAL TABLE dwd_traffic_error_inc
(
    `province_id`     STRING COMMENT '地区编码',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `operate_system`  STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `page_item`       STRING COMMENT '目标id ',
    `page_item_type`  STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页类型',
    `page_id`         STRING COMMENT '页面ID ',
    `source_type`     STRING COMMENT '来源类型',
    `entry`           STRING COMMENT 'icon手机图标  notice 通知',
    `loading_time`    STRING COMMENT '启动加载时间',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `open_ad_ms`      STRING COMMENT '广告总共播放时间',
    `open_ad_skip_ms` STRING COMMENT '用户跳过广告时点',
    `actions`         ARRAY<STRUCT<action_id:STRING,item:STRING,item_type:STRING,ts:BIGINT>> COMMENT '动作信息',
    `displays`        ARRAY<STRUCT<display_type :STRING,item :STRING,item_type :STRING,`order` :STRING,pos_id
                                   :STRING>> COMMENT '曝光信息',
    `date_id`         STRING COMMENT '日期id',
    `error_time`      STRING COMMENT '错误时间',
    `error_code`      STRING COMMENT '错误码',
    `error_msg`       STRING COMMENT '错误信息'
) COMMENT '错误日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_traffic_error_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--数据装载
set hive.cbo.enable=false;
set hive.execution.engine=mr;
insert overwrite table dwd_traffic_error_inc partition (dt = '2020-06-14')
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
       entry,
       loading_time,
       open_ad_id,
       open_ad_ms,
       open_ad_skip_ms,
       actions,
       displays,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') error_time,
       error_code,
       error_msg
from (
         select common.ar      area_code,
                common.ba      brand,
                common.ch      channel,
                common.is_new,
                common.md      model,
                common.mid     mid_id,
                common.os      operate_system,
                common.uid     user_id,
                common.vc      version_code,
                page.during_time,
                page.item      page_item,
                page.item_type page_item_type,
                page.last_page_id,
                page.page_id,
                page.source_type,
                `start`.entry,
                `start`.loading_time,
                `start`.open_ad_id,
                `start`.open_ad_ms,
                `start`.open_ad_skip_ms,
                actions,
                displays,
                err.error_code,
                err.msg        error_msg,
                ts
         from ods_log_inc
         where dt = '2020-06-14'
           and err is not null
     ) log
         join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;
