-- 创建表
DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `id`               STRING COMMENT '编号',
    `user_id`          STRING COMMENT '用户id',
    `sku_id`           STRING COMMENT '商品id',
    `date_id`          STRING COMMENT '时间id',
    `create_time`      STRING COMMENT '加购时间',
    `source_id`        STRING COMMENT '来源类型ID',
    `source_type_code` STRING COMMENT '来源类型编码',
    `source_type_name` STRING COMMENT '来源类型名称',
    `sku_num`          BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购物车事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select id,
       user_id,
       sku_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       source_id,
       source_type,
       dic.dic_name,
       sku_num,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.user_id,
                data.sku_id,
                data.create_time,
                data.source_id,
                data.source_type,
                data.sku_num
         from ods_cart_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ci
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) dic
     on ci.source_type = dic.dic_code;

--每日装载
insert overwrite table dwd_trade_cart_add_inc partition (dt = '2020-06-15')
select id,
       user_id,
       sku_id,
       date_id,
       create_time,
       source_id,
       source_type_code,
       source_type_name,
       sku_num
from (
         select data.id,
                data.user_id,
                data.sku_id,
                date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd')          date_id,
                date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') create_time,
                data.source_id,
                data.source_type                                                           source_type_code,
                if(type = 'insert', data.sku_num, data.sku_num - old['sku_num'])           sku_num
         from ods_cart_info_inc
         where dt = '2020-06-15'
           and (type = 'insert'
             or (type = 'update' and old['sku_num'] is not null and data.sku_num > cast(old['sku_num'] as int)))
     ) cart
         left join
     (
         select dic_code,
                dic_name source_type_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) dic
     on cart.source_type_code = dic.dic_code;
