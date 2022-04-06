-- 创建表
DROP TABLE IF EXISTS dim_province_full;
CREATE EXTERNAL TABLE dim_province_full
(
    `id`            STRING COMMENT 'id',
    `province_name` STRING COMMENT '省市名称',
    `area_code`     STRING COMMENT '地区编码',
    `iso_code`      STRING COMMENT '旧版ISO-3166-2编码，供可视化使用',
    `iso_3166_2`    STRING COMMENT '新版IOS-3166-2编码，供可视化使用',
    `region_id`     STRING COMMENT '地区id',
    `region_name`   STRING COMMENT '地区名称'
) COMMENT '地区维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_province_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--数据装载
insert overwrite table dim_province_full partition (dt = '2020-06-14')
select province.id,
       province.name,
       province.area_code,
       province.iso_code,
       province.iso_3166_2,
       region_id,
       region_name
from (
         select id,
                name,
                region_id,
                area_code,
                iso_code,
                iso_3166_2
         from ods_base_province_full
         where dt = '2020-06-14'
     ) province
         left join
     (
         select id,
                region_name
         from ods_base_region_full
         where dt = '2020-06-14'
     ) region
     on province.region_id = region.id;
