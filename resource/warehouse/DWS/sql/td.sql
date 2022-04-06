--交易域用户粒度订单历史至今汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_user_order_td;
CREATE EXTERNAL TABLE dws_trade_user_order_td
(
    `user_id`                   STRING COMMENT '用户id',
    `order_date_first`          STRING COMMENT '首次下单日期',
    `order_date_last`           STRING COMMENT '末次下单日期',
    `order_count_td`            BIGINT COMMENT '下单次数',
    `order_num_td`              BIGINT COMMENT '购买商品件数',
    `original_amount_td`        DECIMAL(16, 2) COMMENT '原始金额',
    `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT '活动优惠金额',
    `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '优惠券优惠金额',
    `total_amount_td`           DECIMAL(16, 2) COMMENT '最终金额'
) COMMENT '交易域用户粒度订单历史至今汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2020-06-14')
select user_id,
       min(dt)                        login_date_first,
       max(dt)                        login_date_last,
       sum(order_count_1d)            order_count,
       sum(order_num_1d)              order_num,
       sum(order_original_amount_1d)  original_amount,
       sum(activity_reduce_amount_1d) activity_reduce_amount,
       sum(coupon_reduce_amount_1d)   coupon_reduce_amount,
       sum(order_total_amount_1d)     total_amount
from dws_trade_user_order_1d
group by user_id;

--每日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2020-06-15')
select nvl(old.user_id, new.user_id),
       if(new.user_id is not null and old.user_id is null, '2020-06-15', old.order_date_first),
       if(new.user_id is not null, '2020-06-15', old.order_date_last),
       nvl(old.order_count_td, 0) + nvl(new.order_count_1d, 0),
       nvl(old.order_num_td, 0) + nvl(new.order_num_1d, 0),
       nvl(old.original_amount_td, 0) + nvl(new.order_original_amount_1d, 0),
       nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0),
       nvl(old.coupon_reduce_amount_td, 0) + nvl(new.coupon_reduce_amount_1d, 0),
       nvl(old.total_amount_td, 0) + nvl(new.order_total_amount_1d, 0)
from (
         select user_id,
                order_date_first,
                order_date_last,
                order_count_td,
                order_num_td,
                original_amount_td,
                activity_reduce_amount_td,
                coupon_reduce_amount_td,
                total_amount_td
         from dws_trade_user_order_td
         where dt = date_add('2020-06-15', -1)
     ) old
         full outer join
     (
         select user_id,
                order_count_1d,
                order_num_1d,
                order_original_amount_1d,
                activity_reduce_amount_1d,
                coupon_reduce_amount_1d,
                order_total_amount_1d
         from dws_trade_user_order_1d
         where dt = '2020-06-15'
     ) new
     on old.user_id = new.user_id;

--交易域用户粒度支付历史至今汇总表
--建表语句
DROP TABLE IF EXISTS dws_trade_user_payment_td;
CREATE EXTERNAL TABLE dws_trade_user_payment_td
(
    `user_id`            STRING COMMENT '用户id',
    `payment_date_first` STRING COMMENT '首次支付日期',
    `payment_date_last`  STRING COMMENT '末次支付日期',
    `payment_count_td`   BIGINT COMMENT '最近7日支付次数',
    `payment_num_td`     BIGINT COMMENT '最近7日支付商品件数',
    `payment_amount_td`  DECIMAL(16, 2) COMMENT '最近7日支付金额'
) COMMENT '交易域用户粒度支付历史至今汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日装载
insert overwrite table dws_trade_user_payment_td partition (dt = '2020-06-14')
select user_id,
       min(dt)                payment_date_first,
       max(dt)                payment_date_last,
       sum(payment_count_1d)  payment_count,
       sum(payment_num_1d)    payment_num,
       sum(payment_amount_1d) payment_amount
from dws_trade_user_payment_1d
group by user_id;

--每日装载
insert overwrite table dws_trade_user_payment_td partition (dt = '2020-06-15')
select nvl(old.user_id, new.user_id),
       if(old.user_id is null and new.user_id is not null, '2020-06-15', old.payment_date_first),
       if(new.user_id is not null, '2020-06-15', old.payment_date_last),
       nvl(old.payment_count_td, 0) + nvl(new.payment_count_1d, 0),
       nvl(old.payment_num_td, 0) + nvl(new.payment_num_1d, 0),
       nvl(old.payment_amount_td, 0) + nvl(new.payment_amount_1d, 0)
from (
         select user_id,
                payment_date_first,
                payment_date_last,
                payment_count_td,
                payment_num_td,
                payment_amount_td
         from dws_trade_user_payment_td
         where dt = date_add('2020-06-15', -1)
     ) old
         full outer join
     (
         select user_id,
                payment_count_1d,
                payment_num_1d,
                payment_amount_1d
         from dws_trade_user_payment_1d
         where dt = '2020-06-15'
     ) new
     on old.user_id = new.user_id;

--用户域用户粒度登录历史至今汇总表
--建表语句
DROP TABLE IF EXISTS dws_user_user_login_td;
CREATE EXTERNAL TABLE dws_user_user_login_td
(
    `user_id`         STRING COMMENT '用户id',
    `login_date_last` STRING COMMENT '末次登录日期',
    `login_count_td`  BIGINT COMMENT '累计登录次数'
) COMMENT '用户域用户粒度登录历史至今汇总事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_user_user_login_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--首日装载
insert overwrite table dws_user_user_login_td partition (dt = '2020-06-14')
select u.id,
       nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')),
       nvl(login_count_td, 1)
from (
         select id,
                create_time
         from dim_user_zip
         where dt = '9999-12-31'
     ) u
         left join
     (
         select user_id,
                max(dt)  login_date_last,
                count(*) login_count_td
         from dwd_user_login_inc
         group by user_id
     ) l
     on u.id = l.user_id;

--每日装载
insert overwrite table dws_user_user_login_td partition (dt = '2020-06-15')
select nvl(old.user_id, new.user_id),
       if(new.user_id is null, old.login_date_last, '2020-06-15'),
       nvl(old.login_count_td, 0) + nvl(new.login_count_1d, 0)
from (
         select user_id,
                login_date_last,
                login_count_td
         from dws_user_user_login_td
         where dt = date_add('2020-06-15', -1)
     ) old
         full outer join
     (
         select user_id,
                count(*) login_count_1d
         from dwd_user_login_inc
         where dt = '2020-06-15'
         group by user_id
     ) new
     on old.user_id = new.user_id;
