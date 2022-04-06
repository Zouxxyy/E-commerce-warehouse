--建表语句
DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单id',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT '商品id',
    `province_id`           STRING COMMENT '省份id',
    `activity_id`           STRING COMMENT '参与活动规则id',
    `activity_rule_id`      STRING COMMENT '参与活动规则id',
    `coupon_id`             STRING COMMENT '使用优惠券id',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`               STRING COMMENT '支付日期id',
    `callback_time`         STRING COMMENT '支付成功时间',
    `source_id`             STRING COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域成功支付事务事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日装载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       source_id,
       source_type,
       src_dic.dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount,
       date_format(callback_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
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
         join
     (
         select data.user_id,
                data.order_id,
                data.payment_type,
                data.callback_time
         from ods_payment_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
           and data.payment_status = '1602'
     ) pi
     on od.order_id = pi.order_id
         left join
     (
         select data.id,
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
           and parent_code = '11'
     ) pay_dic
     on pi.payment_type = pay_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '24'
     ) src_dic
     on od.source_type = src_dic.dic_code;

--每日装载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt = '2020-06-15')
select od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       source_id,
       source_type,
       src_dic.dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.source_id,
                data.source_type,
                data.sku_num,
                data.sku_num * data.order_price split_original_amount,
                data.split_total_amount,
                data.split_activity_amount,
                data.split_coupon_amount
         from ods_order_detail_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) od
         join
     (
         select data.user_id,
                data.order_id,
                data.payment_type,
                data.callback_time
         from ods_payment_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and array_contains(map_keys(old), 'payment_status')
           and data.payment_status = '1602'
     ) pi
     on od.order_id = pi.order_id
         left join
     (
         select data.id,
                data.province_id
         from ods_order_info_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) oi
     on od.order_id = oi.id
         left join
     (
         select data.order_detail_id,
                data.activity_id,
                data.activity_rule_id
         from ods_order_detail_activity_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) act
     on od.id = act.order_detail_id
         left join
     (
         select data.order_detail_id,
                data.coupon_id
         from ods_order_detail_coupon_inc
         where (dt = '2020-06-15' or dt = date_add('2020-06-15', -1))
           and (type = 'insert' or type = 'bootstrap-insert')
     ) cou
     on od.id = cou.order_detail_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '11'
     ) pay_dic
     on pi.payment_type = pay_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '24'
     ) src_dic
     on od.source_type = src_dic.dic_code;
