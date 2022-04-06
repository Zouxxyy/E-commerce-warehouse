-- 创建表
DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip
(
    `id`           STRING COMMENT '用户id',
    `login_name`   STRING COMMENT '用户名称',
    `nick_name`    STRING COMMENT '用户昵称',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_user_zip/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


--首日装载
insert overwrite table dim_user_zip partition (dt = '9999-12-31')
select data.id,
       data.login_name,
       data.nick_name,
       md5(data.name),
       md5(data.phone_num),
       md5(data.email),
       data.user_level,
       data.birthday,
       data.gender,
       data.create_time,
       data.operate_time,
       '2020-06-14' start_date,
       '9999-12-31' end_date
from ods_user_info_inc
where dt = '2020-06-14'
  and type = 'bootstrap-insert';


--每日装载
with tmp as
         (
             select old.id           old_id,
                    old.login_name   old_login_name,
                    old.nick_name    old_nick_name,
                    old.name         old_name,
                    old.phone_num    old_phone_num,
                    old.email        old_email,
                    old.user_level   old_user_level,
                    old.birthday     old_birthday,
                    old.gender       old_gender,
                    old.create_time  old_create_time,
                    old.operate_time old_operate_time,
                    old.start_date   old_start_date,
                    old.end_date     old_end_date,
                    new.id           new_id,
                    new.login_name   new_login_name,
                    new.nick_name    new_nick_name,
                    new.name         new_name,
                    new.phone_num    new_phone_num,
                    new.email        new_email,
                    new.user_level   new_user_level,
                    new.birthday     new_birthday,
                    new.gender       new_gender,
                    new.create_time  new_create_time,
                    new.operate_time new_operate_time,
                    new.start_date   new_start_date,
                    new.end_date     new_end_date
             from (
                      select id,
                             login_name,
                             nick_name,
                             name,
                             phone_num,
                             email,
                             user_level,
                             birthday,
                             gender,
                             create_time,
                             operate_time,
                             start_date,
                             end_date
                      from dim_user_zip
                      where dt = '9999-12-31'
                  ) old
                      full outer join
                  (
                      select id,
                             login_name,
                             nick_name,
                             md5(name)      name,
                             md5(phone_num) phone_num,
                             md5(email)     email,
                             user_level,
                             birthday,
                             gender,
                             create_time,
                             operate_time,
                             '2020-06-15'   start_date,
                             '9999-12-31'   end_date
                      from (
                               select data.id,
                                      data.login_name,
                                      data.nick_name,
                                      data.name,
                                      data.phone_num,
                                      data.email,
                                      data.user_level,
                                      data.birthday,
                                      data.gender,
                                      data.create_time,
                                      data.operate_time,
                                      row_number() over (partition by data.id order by ts desc) rn
                               from ods_user_info_inc
                               where dt = '2020-06-15'
                           ) t1
                      where rn = 1
                  ) new
                  on old.id = new.id
         )
insert
overwrite
table
dim_user_zip
partition
(
dt
)
select if(new_id is not null, new_id, old_id),
       if(new_id is not null, new_login_name, old_login_name),
       if(new_id is not null, new_nick_name, old_nick_name),
       if(new_id is not null, new_name, old_name),
       if(new_id is not null, new_phone_num, old_phone_num),
       if(new_id is not null, new_email, old_email),
       if(new_id is not null, new_user_level, old_user_level),
       if(new_id is not null, new_birthday, old_birthday),
       if(new_id is not null, new_gender, old_gender),
       if(new_id is not null, new_create_time, old_create_time),
       if(new_id is not null, new_operate_time, old_operate_time),
       if(new_id is not null, new_start_date, old_start_date),
       if(new_id is not null, new_end_date, old_end_date),
       if(new_id is not null, new_end_date, old_end_date) dt
from tmp
union all
select old_id,
       old_login_name,
       old_nick_name,
       old_name,
       old_phone_num,
       old_email,
       old_user_level,
       old_birthday,
       old_gender,
       old_create_time,
       old_operate_time,
       old_start_date,
       cast(date_add('2020-06-15', -1) as string) old_end_date,
       cast(date_add('2020-06-15', -1) as string) dt
from tmp
where old_id is not null
  and new_id is not null;
