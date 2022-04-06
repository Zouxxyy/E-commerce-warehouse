#!/bin/bash

DATAX_PATH='/home/zxy/software/datax/script'

python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_activity_stats
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_coupon_stats
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_new_buyer_stats
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_order_by_province
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_page_path
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_repeat_purchase_by_tm
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_sku_cart_num_top3_by_cate
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_trade_stats
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_trade_stats_by_cate
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_trade_stats_by_tm
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_traffic_stats_by_channel
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_user_action
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_user_change
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_user_retention
python ${DATAX_PATH}/gen_export_config.py -d gmall_report -t ads_user_stats
