--建表语句
DROP TABLE IF EXISTS dwd_tool_coupon_get_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_get_inc
(
    `id`        STRING COMMENT '编号',
    `coupon_id` STRING COMMENT '优惠券ID',
    `user_id`   STRING COMMENT 'userid',
    `date_id`   STRING COMMENT '日期ID',
    `get_time`  STRING COMMENT '领取时间'
) COMMENT '优惠券领取事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_get_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

--首日装载
insert overwrite table dwd_tool_coupon_get_inc partition (dt)
select data.id,
       data.coupon_id,
       data.user_id,
       date_format(data.get_time, 'yyyy-MM-dd') date_id,
       data.get_time,
       date_format(data.get_time, 'yyyy-MM-dd')
from ods_coupon_use_inc
where dt = '2020-06-14'
  and type = 'bootstrap-insert';

--每日装载
insert overwrite table dwd_tool_coupon_get_inc partition (dt = '2020-06-15')
select data.id,
       data.coupon_id,
       data.user_id,
       date_format(data.get_time, 'yyyy-MM-dd') date_id,
       data.get_time
from ods_coupon_use_inc
where dt = '2020-06-15'
  and type = 'insert';
