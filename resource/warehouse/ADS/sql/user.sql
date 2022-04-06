--用户变动统计
--建表语句
DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_change/';

--数据装载
insert overwrite table ads_user_change
select *
from ads_user_change
union
select churn.dt,
       user_churn_count,
       user_back_count
from (
         select '2020-06-14' dt,
                count(*)     user_churn_count
         from dws_user_user_login_td
         where dt = '2020-06-14'
           and login_date_last = date_add('2020-06-14', -7)
     ) churn
         join
     (
         select '2020-06-14' dt,
                count(*)     user_back_count
         from (
                  select user_id,
                         login_date_last
                  from dws_user_user_login_td
                  where dt = '2020-06-14'
              ) t1
                  join
              (
                  select user_id,
                         login_date_last login_date_previous
                  from dws_user_user_login_td
                  where dt = date_add('2020-06-14', -1)
              ) t2
              on t1.user_id = t2.user_id
         where datediff(login_date_last, login_date_previous) >= 8
     ) back
     on churn.dt = back.dt;

--用户留存率
--建表语句
DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_retention/';

--数据装载
insert overwrite table ads_user_retention
select *
from ads_user_retention
union
select '2020-06-14'                                                                           dt,
       login_date_first                                                                       create_date,
       datediff('2020-06-14', login_date_first)                                               retention_day,
       sum(if(login_date_last = '2020-06-14', 1, 0))                                          retention_count,
       count(*)                                                                               new_user_count,
       cast(sum(if(login_date_last = '2020-06-14', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                date_id login_date_first
         from dwd_user_register_inc
         where dt >= date_add('2020-06-14', -7)
           and dt < '2020-06-14'
     ) t1
         join
     (
         select user_id,
                login_date_last
         from dws_user_user_login_td
         where dt = '2020-06-14'
     ) t2
     on t1.user_id = t2.user_id
group by login_date_first;

--用户新增活跃统计
--建表语句
DROP TABLE IF EXISTS ads_user_stats;
CREATE EXTERNAL TABLE ads_user_stats
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_stats/';

--数据装载
insert overwrite table ads_user_stats
select *
from ads_user_stats
union
select '2020-06-14' dt,
       t1.recent_days,
       new_user_count,
       active_user_count
from (
         select recent_days,
                sum(if(login_date_last >= date_add('2020-06-14', -recent_days + 1), 1, 0)) new_user_count
         from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
         group by recent_days
     ) t1
         join
     (
         select recent_days,
                sum(if(date_id >= date_add('2020-06-14', -recent_days + 1), 1, 0)) active_user_count
         from dwd_user_register_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
         group by recent_days
     ) t2
     on t1.recent_days = t2.recent_days;

--用户行为漏斗分析
--建表语句
DROP TABLE IF EXISTS ads_user_action;
CREATE EXTERNAL TABLE ads_user_action
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加入购物车人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
) COMMENT '漏斗分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_action/';

--数据装载
insert overwrite table ads_user_action
select *
from ads_user_action
union
select '2020-06-14' dt,
       page.recent_days,
       home_count,
       good_detail_count,
       cart_count,
       order_count,
       payment_count
from (
         select 1                                      recent_days,
                sum(if(page_id = 'home', 1, 0))        home_count,
                sum(if(page_id = 'good_detail', 1, 0)) good_detail_count
         from dws_traffic_page_visitor_page_view_1d
         where dt = '2020-06-14'
           and page_id in ('home', 'good_detail')
         union all
         select recent_days,
                sum(if(page_id = 'home' and view_count > 0, 1, 0)),
                sum(if(page_id = 'good_detail' and view_count > 0, 1, 0))
         from (
                  select recent_days,
                         page_id,
                         case recent_days
                             when 7 then view_count_7d
                             when 30 then view_count_30d
                             end view_count
                  from dws_traffic_page_visitor_page_view_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
                    and page_id in ('home', 'good_detail')
              ) t1
         group by recent_days
     ) page
         join
     (
         select 1        recent_days,
                count(*) cart_count
         from dws_trade_user_cart_add_1d
         where dt = '2020-06-14'
         union all
         select recent_days,
                sum(if(cart_count > 0, 1, 0))
         from (
                  select recent_days,
                         case recent_days
                             when 7 then cart_add_count_7d
                             when 30 then cart_add_count_30d
                             end cart_count
                  from dws_trade_user_cart_add_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days
     ) cart
     on page.recent_days = cart.recent_days
         join
     (
         select 1        recent_days,
                count(*) order_count
         from dws_trade_user_order_1d
         where dt = '2020-06-14'
         union all
         select recent_days,
                sum(if(order_count > 0, 1, 0))
         from (
                  select recent_days,
                         case recent_days
                             when 7 then order_count_7d
                             when 30 then order_count_30d
                             end order_count
                  from dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days
     ) ord
     on page.recent_days = ord.recent_days
         join
     (
         select 1        recent_days,
                count(*) payment_count
         from dws_trade_user_payment_1d
         where dt = '2020-06-14'
         union all
         select recent_days,
                sum(if(order_count > 0, 1, 0))
         from (
                  select recent_days,
                         case recent_days
                             when 7 then payment_count_7d
                             when 30 then payment_count_30d
                             end order_count
                  from dws_trade_user_payment_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days
     ) pay
     on page.recent_days = pay.recent_days;

--新增交易用户统计
--建表语句
DROP TABLE IF EXISTS ads_new_buyer_stats;
CREATE EXTERNAL TABLE ads_new_buyer_stats
(
    `dt`                     STRING COMMENT '统计日期',
    `recent_days`            BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count`   BIGINT COMMENT '新增下单人数',
    `new_payment_user_count` BIGINT COMMENT '新增支付人数'
) COMMENT '新增交易用户统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_new_buyer_stats/';

--数据装载
insert overwrite table ads_new_buyer_stats
select *
from ads_new_buyer_stats
union
select '2020-06-14',
       odr.recent_days,
       new_order_user_count,
       new_payment_user_count
from (
         select recent_days,
                sum(if(order_date_first >= date_add('2020-06-14', -recent_days + 1), 1, 0)) new_order_user_count
         from dws_trade_user_order_td lateral view explode(array(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
         group by recent_days
     ) odr
         join
     (
         select recent_days,
                sum(if(payment_date_first >= date_add('2020-06-14', -recent_days + 1), 1, 0)) new_payment_user_count
         from dws_trade_user_payment_td lateral view explode(array(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
         group by recent_days
     ) pay
     on odr.recent_days = pay.recent_days;
