-- 创建表
DROP TABLE IF EXISTS dim_coupon_full;
CREATE
    EXTERNAL TABLE dim_coupon_full
(
    `id`               STRING COMMENT '购物券编号',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type_code` STRING COMMENT '购物券类型编码',
    `coupon_type_name` STRING COMMENT '购物券类型名称',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `benefit_rule`     STRING COMMENT '优惠规则:满元*减*元，满*件打*折',
    `create_time`      STRING COMMENT '创建时间',
    `range_type_code`  STRING COMMENT '优惠范围类型编码',
    `range_type_name`  STRING COMMENT '优惠范围类型名称',
    `limit_num`        BIGINT COMMENT '最多领取次数',
    `taken_count`      BIGINT COMMENT '已领取次数',
    `start_time`       STRING COMMENT '可以领取的开始日期',
    `end_time`         STRING COMMENT '可以领取的结束日期',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
) COMMENT '优惠券维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_coupon_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dim_coupon_full partition (dt = '2020-06-14')
select id,
       coupon_name,
       coupon_type,
       coupon_dic.dic_name,
       condition_amount,
       condition_num,
       activity_id,
       benefit_amount,
       benefit_discount,
       case coupon_type
           when '3201' then concat('满', condition_amount, '元减', benefit_amount, '元')
           when '3202' then concat('满', condition_num, '件打', 10 * (1 - benefit_discount), '折')
           when '3203' then concat('减', benefit_amount, '元')
           end benefit_rule,
       create_time,
       range_type,
       range_dic.dic_name,
       limit_num,
       taken_count,
       start_time,
       end_time,
       operate_time,
       expire_time
from (
         select id,
                coupon_name,
                coupon_type,
                condition_amount,
                condition_num,
                activity_id,
                benefit_amount,
                benefit_discount,
                create_time,
                range_type,
                limit_num,
                taken_count,
                start_time,
                end_time,
                operate_time,
                expire_time
         from ods_coupon_info_full
         where dt = '2020-06-14'
     ) ci
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '32'
     ) coupon_dic
     on ci.coupon_type = coupon_dic.dic_code
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '33'
     ) range_dic
     on ci.range_type = range_dic.dic_code;

