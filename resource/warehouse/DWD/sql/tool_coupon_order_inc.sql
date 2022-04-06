--建表语句
DROP TABLE IF EXISTS dwd_tool_coupon_order_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_order_inc
(
    `id`         STRING COMMENT '编号',
    `coupon_id`  STRING COMMENT '优惠券ID',
    `user_id`    STRING COMMENT 'user_id',
    `order_id`   STRING COMMENT 'order_id',
    `date_id`    STRING COMMENT '日期ID',
    `order_time` STRING COMMENT '使用下单时间'
) COMMENT '优惠券使用下单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_tool_coupon_order_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

--首日装载
insert overwrite table dwd_tool_coupon_order_inc partition (dt)
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.using_time, 'yyyy-MM-dd') date_id,
       data.using_time,
       date_format(data.using_time, 'yyyy-MM-dd')
from ods_coupon_use_inc
where dt = '2020-06-14'
  and type = 'bootstrap-insert'
  and data.using_time is not null;

--每日装载
insert overwrite table dwd_tool_coupon_order_inc partition (dt = '2020-06-15')
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.using_time, 'yyyy-MM-dd') date_id,
       data.using_time
from ods_coupon_use_inc
where dt = '2020-06-15'
  and type = 'update'
  and array_contains(map_keys(old), 'using_time');
