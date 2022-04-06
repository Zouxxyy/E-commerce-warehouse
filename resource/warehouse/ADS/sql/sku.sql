--最近7/30日各品牌复购率
--建表语句
DROP TABLE IF EXISTS ads_repeat_purchase_by_tm;
CREATE EXTERNAL TABLE ads_repeat_purchase_by_tm
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,7:最近7天,30:最近30天',
    `tm_id`             STRING COMMENT '品牌ID',
    `tm_name`           STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
) COMMENT '各品牌复购率统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_repeat_purchase_by_tm/';

--数据装载
insert overwrite table ads_repeat_purchase_by_tm
select *
from ads_repeat_purchase_by_tm
union
select '2020-06-14' dt,
       recent_days,
       tm_id,
       tm_name,
       cast(sum(if(order_count >= 2, 1, 0)) / sum(if(order_count >= 1, 1, 0)) as decimal(16, 2))
from (
         select '2020-06-14'     dt,
                recent_days,
                user_id,
                tm_id,
                tm_name,
                sum(order_count) order_count
         from (
                  select recent_days,
                         user_id,
                         tm_id,
                         tm_name,
                         case recent_days
                             when 7 then order_count_7d
                             when 30 then order_count_30d
                             end order_count
                  from dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, user_id, tm_id, tm_name
     ) t2
group by recent_days, tm_id, tm_name;

--各品牌商品交易统计
--建表语句
DROP TABLE IF EXISTS ads_trade_stats_by_tm;
CREATE EXTERNAL TABLE ads_trade_stats_by_tm
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`                   STRING COMMENT '品牌ID',
    `tm_name`                 STRING COMMENT '品牌名称',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '订单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
) COMMENT '各品牌商品交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_trade_stats_by_tm/';

--数据装载
insert overwrite table ads_trade_stats_by_tm
select *
from ads_trade_stats_by_tm
union
select '2020-06-14' dt,
       nvl(odr.recent_days, refund.recent_days),
       nvl(odr.tm_id, refund.tm_id),
       nvl(odr.tm_name, refund.tm_name),
       nvl(order_count, 0),
       nvl(order_user_count, 0),
       nvl(order_refund_count, 0),
       nvl(order_refund_user_count, 0)
from (
         select 1                         recent_days,
                tm_id,
                tm_name,
                sum(order_count_1d)       order_count,
                count(distinct (user_id)) order_user_count
         from dws_trade_user_sku_order_1d
         where dt = '2020-06-14'
         group by tm_id, tm_name
         union all
         select recent_days,
                tm_id,
                tm_name,
                sum(order_count),
                count(distinct (if(order_count > 0, user_id, null)))
         from (
                  select recent_days,
                         user_id,
                         tm_id,
                         tm_name,
                         case recent_days
                             when 7 then order_count_7d
                             when 30 then order_count_30d
                             end order_count
                  from dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, tm_id, tm_name
     ) odr
         full outer join
     (
         select 1                          recent_days,
                tm_id,
                tm_name,
                sum(order_refund_count_1d) order_refund_count,
                count(distinct (user_id))  order_refund_user_count
         from dws_trade_user_sku_order_refund_1d
         where dt = '2020-06-14'
         group by tm_id, tm_name
         union all
         select recent_days,
                tm_id,
                tm_name,
                sum(order_refund_count),
                count(if(order_refund_count > 0, user_id, null))
         from (
                  select recent_days,
                         user_id,
                         tm_id,
                         tm_name,
                         case recent_days
                             when 7 then order_refund_count_7d
                             when 30 then order_refund_count_30d
                             end order_refund_count
                  from dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, tm_id, tm_name
     ) refund
     on odr.recent_days = refund.recent_days
         and odr.tm_id = refund.tm_id
         and odr.tm_name = refund.tm_name;

--各品类商品交易统计
--建表语句
DROP TABLE IF EXISTS ads_trade_stats_by_cate;
CREATE EXTERNAL TABLE ads_trade_stats_by_cate
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`            STRING COMMENT '一级分类id',
    `category1_name`          STRING COMMENT '一级分类名称',
    `category2_id`            STRING COMMENT '二级分类id',
    `category2_name`          STRING COMMENT '二级分类名称',
    `category3_id`            STRING COMMENT '三级分类id',
    `category3_name`          STRING COMMENT '三级分类名称',
    `order_count`             BIGINT COMMENT '订单数',
    `order_user_count`        BIGINT COMMENT '订单人数',
    `order_refund_count`      BIGINT COMMENT '退单数',
    `order_refund_user_count` BIGINT COMMENT '退单人数'
) COMMENT '各分类商品交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_trade_stats_by_cate/';

--数据装载
insert overwrite table ads_trade_stats_by_cate
select *
from ads_trade_stats_by_cate
union
select '2020-06-14' dt,
       nvl(odr.recent_days, refund.recent_days),
       nvl(odr.category1_id, refund.category1_id),
       nvl(odr.category1_name, refund.category1_name),
       nvl(odr.category2_id, refund.category2_id),
       nvl(odr.category2_name, refund.category2_name),
       nvl(odr.category3_id, refund.category3_id),
       nvl(odr.category3_name, refund.category3_name),
       nvl(order_count, 0),
       nvl(order_user_count, 0),
       nvl(order_refund_count, 0),
       nvl(order_refund_user_count, 0)
from (
         select 1                         recent_days,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                sum(order_count_1d)       order_count,
                count(distinct (user_id)) order_user_count
         from dws_trade_user_sku_order_1d
         where dt = '2020-06-14'
         group by category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
         union all
         select recent_days,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                sum(order_count),
                count(distinct (if(order_count > 0, user_id, null)))
         from (
                  select recent_days,
                         user_id,
                         category1_id,
                         category1_name,
                         category2_id,
                         category2_name,
                         category3_id,
                         category3_name,
                         case recent_days
                             when 7 then order_count_7d
                             when 30 then order_count_30d
                             end order_count
                  from dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
     ) odr
         full outer join
     (
         select 1                          recent_days,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                sum(order_refund_count_1d) order_refund_count,
                count(distinct (user_id))  order_refund_user_count
         from dws_trade_user_sku_order_refund_1d
         where dt = '2020-06-14'
         group by category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
         union all
         select recent_days,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                sum(order_refund_count),
                count(distinct (if(order_refund_count > 0, user_id, null)))
         from (
                  select recent_days,
                         user_id,
                         category1_id,
                         category1_name,
                         category2_id,
                         category2_name,
                         category3_id,
                         category3_name,
                         case recent_days
                             when 7 then order_refund_count_7d
                             when 30 then order_refund_count_30d
                             end order_refund_count
                  from dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
     ) refund
     on odr.recent_days = refund.recent_days
         and odr.category1_id = refund.category1_id
         and odr.category1_name = refund.category1_name
         and odr.category2_id = refund.category2_id
         and odr.category2_name = refund.category2_name

--各分类商品购物车存量Top10
--建表语句
DROP TABLE IF EXISTS ads_sku_cart_num_top3_by_cate;
CREATE EXTERNAL TABLE ads_sku_cart_num_top3_by_cate
(
    `dt`             STRING COMMENT '统计日期',
    `category1_id`   STRING COMMENT '一级分类ID',
    `category1_name` STRING COMMENT '一级分类名称',
    `category2_id`   STRING COMMENT '二级分类ID',
    `category2_name` STRING COMMENT '二级分类名称',
    `category3_id`   STRING COMMENT '三级分类ID',
    `category3_name` STRING COMMENT '三级分类名称',
    `sku_id`         STRING COMMENT '商品id',
    `sku_name`       STRING COMMENT '商品名称',
    `cart_num`       BIGINT COMMENT '购物车中商品数量',
    `rk`             BIGINT COMMENT '排名'
) COMMENT '各分类商品购物车存量Top10'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate/';

--数据装载
insert overwrite table ads_sku_cart_num_top3_by_cate
select *
from ads_sku_cart_num_top3_by_cate
union
select '2020-06-14' dt,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       sku_id,
       sku_name,
       cart_num,
       rk
from (
         select sku_id,
                sku_name,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name,
                cart_num,
                rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
         from (
                  select sku_id,
                         sum(sku_num) cart_num
                  from dwd_trade_cart_full
                  where dt = '2020-06-14'
                  group by sku_id
              ) cart
                  left join
              (
                  select id,
                         sku_name,
                         category1_id,
                         category1_name,
                         category2_id,
                         category2_name,
                         category3_id,
                         category3_name
                  from dim_sku_full
                  where dt = '2020-06-14'
              ) sku
              on cart.sku_id = sku.id
     ) t1
where rk <= 3;
