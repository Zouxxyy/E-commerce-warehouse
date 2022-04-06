-- 创建表
DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `date_id`               STRING COMMENT '下单日期id',
    `create_time`           STRING COMMENT '下单时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单明细事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.create_time,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) od
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) dic
     on od.source_type = dic.dic_code;

--每日装载
insert overwrite table dwd_trade_order_detail_inc partition (dt = '2020-06-15')
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_id,
       create_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                date_format(data.create_time, 'yyyy-MM-dd') date_id,
                data.create_time,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price             split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) od
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) dic
     on od.source_type = dic.dic_code;
