-- 建表语句
DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`       STRING COMMENT '编号',
    `user_id`  STRING COMMENT '用户id',
    `sku_id`   STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num`  BIGINT COMMENT '加购物车件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--数据装载
insert overwrite table dwd_trade_cart_full partition (dt = '2020-06-14')
select id,
       user_id,
       sku_id,
       sku_name,
       sku_num
from ods_cart_info_full
where dt = '2020-06-14'
  and is_ordered = '0';
