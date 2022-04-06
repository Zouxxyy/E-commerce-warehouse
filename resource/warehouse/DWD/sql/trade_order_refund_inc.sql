--建表语句
DROP TABLE IF EXISTS dwd_trade_order_refund_inc;
CREATE EXTERNAL TABLE dwd_trade_order_refund_inc
(
    `id`                      STRING COMMENT '编号',
    `user_id`                 STRING COMMENT '用户ID',
    `order_id`                STRING COMMENT '订单ID',
    `sku_id`                  STRING COMMENT '商品ID',
    `province_id`             STRING COMMENT '地区ID',
    `date_id`                 STRING COMMENT '日期ID',
    `create_time`             STRING COMMENT '退单时间',
    `refund_type_code`        STRING COMMENT '退单类型编码',
    `refund_type_name`        STRING COMMENT '退单类型名称',
    `refund_reason_type_code` STRING COMMENT '退单原因类型编码',
    `refund_reason_type_name` STRING COMMENT '退单原因类型名称',
    `refund_reason_txt`       STRING COMMENT '退单原因描述',
    `refund_num`              BIGINT COMMENT '退单件数',
    `refund_amount`           DECIMAL(16, 2) COMMENT '退单金额'
) COMMENT '交易域退单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_refund_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

--首日装载
insert overwrite table dwd_trade_order_refund_inc partition (dt)
select ri.id,
       user_id,
       order_id,
       sku_id,
       province_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       refund_type,
       type_dic.dic_name,
       refund_reason_type,
       reason_dic.dic_name,
       refund_reason_txt,
       refund_num,
       refund_amount,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.user_id,
                data.order_id,
                data.sku_id,
                data.refund_type,
                data.refund_num,
                data.refund_amount,
                data.refund_reason_type,
                data.refund_reason_txt,
                data.create_time
         from ods_order_refund_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ri
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on ri.order_id = oi.id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '15'
     ) type_dic
     on ri.refund_type = type_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '13'
     ) reason_dic
     on ri.refund_reason_type = reason_dic.dic_code;

--每日装载
insert overwrite table dwd_trade_order_refund_inc partition (dt = '2020-06-15')
select ri.id,
       user_id,
       order_id,
       sku_id,
       province_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       refund_type,
       type_dic.dic_name,
       refund_reason_type,
       reason_dic.dic_name,
       refund_reason_txt,
       refund_num,
       refund_amount
from (
         select data.id,
                data.user_id,
                data.order_id,
                data.sku_id,
                data.refund_type,
                data.refund_num,
                data.refund_amount,
                data.refund_reason_type,
                data.refund_reason_txt,
                data.create_time
         from ods_order_refund_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) ri
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.order_status = '1005'
           and array_contains(map_keys(old), 'order_status')
     ) oi
     on ri.order_id = oi.id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '15'
     ) type_dic
     on ri.refund_type = type_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '13'
     ) reason_dic
     on ri.refund_reason_type = reason_dic.dic_code;
