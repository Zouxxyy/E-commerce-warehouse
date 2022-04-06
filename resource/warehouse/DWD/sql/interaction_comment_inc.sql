--建表语句
DROP TABLE IF EXISTS dwd_interaction_comment_inc;
CREATE EXTERNAL TABLE dwd_interaction_comment_inc
(
    `id`            STRING COMMENT '编号',
    `user_id`       STRING COMMENT '用户ID',
    `sku_id`        STRING COMMENT 'sku_id',
    `order_id`      STRING COMMENT '订单ID',
    `date_id`       STRING COMMENT '日期ID',
    `create_time`   STRING COMMENT '评价时间',
    `appraise_code` STRING COMMENT '评价编码',
    `appraise_name` STRING COMMENT '评价名称'
) COMMENT '评价事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_interaction_comment_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

--首日装载
insert overwrite table dwd_interaction_comment_inc partition (dt)
select id,
       user_id,
       sku_id,
       order_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       appraise,
       dic_name,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select data.id,
                data.user_id,
                data.sku_id,
                data.order_id,
                data.create_time,
                data.appraise
         from ods_comment_info_inc
         where dt = '2020-06-14'
           and type = 'bootstrap-insert'
     ) ci
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-14'
           and parent_code = '12'
     ) dic
     on ci.appraise = dic.dic_code;

--每日装载
insert overwrite table dwd_interaction_comment_inc partition (dt = '2020-06-15')
select id,
       user_id,
       sku_id,
       order_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       appraise,
       dic_name
from (
         select data.id,
                data.user_id,
                data.sku_id,
                data.order_id,
                data.create_time,
                data.appraise
         from ods_comment_info_inc
         where dt = '2020-06-15'
           and type = 'insert'
     ) ci
         left join
     (
         select dic_code,
                dic_name
         from ods_base_dic_full
         where dt = '2020-06-15'
           and parent_code = '12'
     ) dic
     on ci.appraise = dic.dic_code;
