--建表语句
DROP TABLE IF EXISTS dwd_user_login_inc;
CREATE
    EXTERNAL TABLE dwd_user_login_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份id',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备id',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_user_login_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

--数据装载
insert overwrite table dwd_user_login_inc partition (dt = '2020-06-14')
select user_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (
         select user_id,
                channel,
                area_code,
                version_code,
                mid_id,
                brand,
                model,
                operate_system,
                ts
         from (
                  select user_id,
                         channel,
                         area_code,
                         version_code,
                         mid_id,
                         brand,
                         model,
                         operate_system,
                         ts,
                         row_number() over (partition by session_id order by ts) rn
                  from (
                           select user_id,
                                  channel,
                                  area_code,
                                  version_code,
                                  mid_id,
                                  brand,
                                  model,
                                  operate_system,
                                  ts,
                                  concat(mid_id, '-', last_value(session_start_point, true)
                                                                 over (partition by mid_id order by ts)) session_id
                           from (
                                    select common.uid                              user_id,
                                           common.ch                               channel,
                                           common.ar                               area_code,
                                           common.vc                               version_code,
                                           common.mid                              mid_id,
                                           common.ba                               brand,
                                           common.md                               model,
                                           common.os                               operate_system,
                                           ts,
                                           if(page.last_page_id is null, ts, null) session_start_point
                                    from ods_log_inc
                                    where dt = '2020-06-14'
                                      and page is not null
                                ) t1
                       ) t2
                  where user_id is not null
              ) t3
         where rn = 1
     ) t4
         left join
     (
         select id province_id,
                area_code
         from ods_base_province_full
         where dt = '2020-06-14'
     ) bp
     on t4.area_code = bp.area_code;
