-- 创建表
DROP TABLE IF EXISTS dim_activity_full;
CREATE EXTERNAL TABLE dim_activity_full
(
    `activity_rule_id`   STRING COMMENT '活动规则ID',
    `activity_id`        STRING COMMENT '活动ID',
    `activity_name`      STRING COMMENT '活动名称',
    `activity_type_code` STRING COMMENT '活动类型编码',
    `activity_type_name` STRING COMMENT '活动类型名称',
    `activity_desc`      STRING COMMENT '活动描述',
    `start_time`         STRING COMMENT '开始时间',
    `end_time`           STRING COMMENT '结束时间',
    `create_time`        STRING COMMENT '创建时间',
    `condition_amount`   DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`      BIGINT COMMENT '满减件数',
    `benefit_amount`     DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount`   DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_rule`       STRING COMMENT '优惠规则',
    `benefit_level`      STRING COMMENT '优惠级别'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_activity_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dim_activity_full partition (dt = '2020-06-14')
select rule.id,
       info.id,
       activity_name,
       rule.activity_type,
       dic.dic_name,
       activity_desc,
       start_time,
       end_time,
       create_time,
       condition_amount,
       condition_num,
       benefit_amount,
       benefit_discount,
       case rule.activity_type
           when '3101' then concat('满', condition_amount, '元减', benefit_amount, '元')
           when '3102' then concat('满', condition_num, '件打', 10 * (1 - benefit_discount), '折')
           when '3103' then concat('打', 10 * (1 - benefit_discount), '折')
           end benefit_rule,
       benefit_level
from (
         select id,
                activity_id,
                activity_type,
                condition_amount,
                condition_num,
                benefit_amount,
                benefit_discount,
                benefit_level
         from ods_activity_rule_full
         where dt = '2020-06-14'
     ) rule
         left join
     (
         select id,
                activity_name,
                activity_type,
                activity_desc,
                start_time,
                end_time,
                create_time
         from ods_activity_info_full
         where dt = '2020-06-14'
     ) info
     on rule.activity_id = info.id
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '31'
     ) dic
     on rule.activity_type = dic.dic_code;
