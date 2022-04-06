--建表语句
DROP TABLE IF EXISTS dwd_trade_refund_pay_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_refund_pay_suc_inc
(
    `id`                STRING COMMENT '编号',
    `user_id`           STRING COMMENT '用户ID',
    `order_id`          STRING COMMENT '订单编号',
    `sku_id`            STRING COMMENT 'SKU编号',
    `province_id`       STRING COMMENT '地区ID',
    `payment_type_code` STRING COMMENT '支付类型编码',
    `payment_type_name` STRING COMMENT '支付类型名称',
    `date_id`           STRING COMMENT '日期ID',
    `callback_time`     STRING COMMENT '支付成功时间',
    `refund_num`        DECIMAL(16, 2) COMMENT '退款件数',
    `refund_amount`     DECIMAL(16, 2) COMMENT '退款金额'
) COMMENT '交易域提交退款成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_refund_pay_suc_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

--首日装载
insert overwrite table dwd_trade_refund_pay_suc_inc partition (dt)
select rp.id,
       user_id,
       rp.order_id,
       rp.sku_id,
       province_id,
       payment_type,
       dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       refund_num,
       total_amount,
       date_format(callback_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.payment_type,
                data.callback_time,
                data.total_amount
         from ods_refund_payment_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
           and data.refund_status = '1602'
     ) rp
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) oi
     on rp.order_id = oi.id
         left join
     (
         select data.order_id,
                data.sku_id,
                data.refund_num
         from ods_order_refund_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ri
     on rp.order_id = ri.order_id
         and rp.sku_id = ri.sku_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '11'
     ) dic
     on rp.payment_type = dic.dic_code;

--每日装载
insert overwrite table dwd_trade_refund_pay_suc_inc partition (dt = '2020-06-15')
select rp.id,
       user_id,
       rp.order_id,
       rp.sku_id,
       province_id,
       payment_type,
       dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       refund_num,
       total_amount
from (
         select data.id,
                data.order_id,
                data.sku_id,
                data.payment_type,
                data.callback_time,
                data.total_amount
         from ods_refund_payment_inc
         where dt = '2020-06-15'
           and type = 'update'
           and array_contains(map_keys(old), 'refund_status')
           and data.refund_status = '1602'
     ) rp
         left join
     (
         select data.id,
                data.user_id,
                data.province_id
         from ods_order_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.order_status = '1006'
           and array_contains(map_keys(old), 'order_status')
     ) oi
     on rp.order_id = oi.id
         left join
     (
         select data.order_id,
                data.sku_id,
                data.refund_num
         from ods_order_refund_info_inc
         where dt = '2020-06-15'
           and type = 'update'
           and data.refund_status = '0705'
           and array_contains(map_keys(old), 'refund_status')
     ) ri
     on rp.order_id = ri.order_id
         and rp.sku_id = ri.sku_id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '11'
     ) dic
     on rp.payment_type = dic.dic_code;
