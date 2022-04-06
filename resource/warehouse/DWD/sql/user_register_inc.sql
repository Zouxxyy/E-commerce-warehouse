--建表语句
DROP TABLE IF EXISTS dwd_user_register_inc;
CREATE EXTERNAL TABLE dwd_user_register_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `create_time`    STRING COMMENT '注册时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份id',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备id',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

--首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_user_register_inc partition (dt)
select ui.user_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id user_id,
                data.create_time
         from ods_user_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ui
         left join
     (
         select common.ar  area_code,
                common.ba  brand,
                common.ch  channel,
                common.md  model,
                common.mid mid_id,
                common.os  operate_system,
                common.uid user_id,
                common.vc  version_code
         from ods_log_inc
         where dt = '2020-06-14'
           and page.page_id = 'register'
           and common.uid is not null
     ) log
     on ui.user_id = log.user_id
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on log.area_code = bp.area_code;

--每日装载
insert overwrite table dwd_user_register_inc partition (dt = '2020-06-15')
select ui.user_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (
         select data.id user_id,
                data.create_time
         from ods_user_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) ui
         left join
     (
         select common.ar  area_code,
                common.ba  brand,
                common.ch  channel,
                common.md  model,
                common.mid mid_id,
                common.os  operate_system,
                common.uid user_id,
                common.vc  version_code
         from ods_log_inc
         where dt = '2020-06-15'
           and page.page_id = 'register'
           and common.uid is not null
     ) log
     on ui.user_id = log.user_id
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-15'
     ) bp
     on log.area_code = bp.area_code;
