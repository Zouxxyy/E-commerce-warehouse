#! /bin/bash

DATAX_HOME=/home/zxy/software/datax

#DataX导出路径不允许存在空文件，该函数作用为清理空文件
handle_export_path() {
  for i in $(hadoop fs -ls -R $1 | awk '{print $8}'); do
    hadoop fs -test -z $i
    if [[ $? -eq 0 ]]; then
      echo "$i文件大小为0，正在删除"
      hadoop fs -rm -r -f $i
    fi
  done
}

#数据导出
export_data() {
  datax_config=$1
  export_dir=$2
  handle_export_path $export_dir
  $DATAX_HOME/bin/datax.py -p"-Dexportdir=$export_dir" $datax_config
}

case $1 in
"ads_new_buyer_stats")
  export_data ${DATAX_HOME}/job/gmall_report.ads_new_buyer_stats.json /warehouse/gmall/ads/ads_new_buyer_stats
  ;;
"ads_order_by_province")
  export_data ${DATAX_HOME}/job/gmall_report.ads_order_by_province.json /warehouse/gmall/ads/ads_order_by_province
  ;;
"ads_page_path")
  export_data ${DATAX_HOME}/job/gmall_report.ads_page_path.json /warehouse/gmall/ads/ads_page_path
  ;;
"ads_repeat_purchase_by_tm")
  export_data ${DATAX_HOME}/job/gmall_report.ads_repeat_purchase_by_tm.json /warehouse/gmall/ads/ads_repeat_purchase_by_tm
  ;;
"ads_trade_stats")
  export_data ${DATAX_HOME}/job/gmall_report.ads_trade_stats.json /warehouse/gmall/ads/ads_trade_stats
  ;;
"ads_trade_stats_by_cate")
  export_data ${DATAX_HOME}/job/gmall_report.ads_trade_stats_by_cate.json /warehouse/gmall/ads/ads_trade_stats_by_cate
  ;;
"ads_trade_stats_by_tm")
  export_data ${DATAX_HOME}/job/gmall_report.ads_trade_stats_by_tm.json /warehouse/gmall/ads/ads_trade_stats_by_tm
  ;;
"ads_traffic_stats_by_channel")
  export_data ${DATAX_HOME}/job/gmall_report.ads_traffic_stats_by_channel.json /warehouse/gmall/ads/ads_traffic_stats_by_channel
  ;;
"ads_user_action")
  export_data ${DATAX_HOME}/job/gmall_report.ads_user_action.json /warehouse/gmall/ads/ads_user_action
  ;;
"ads_user_change")
  export_data ${DATAX_HOME}/job/gmall_report.ads_user_change.json /warehouse/gmall/ads/ads_user_change
  ;;
"ads_user_retention")
  export_data ${DATAX_HOME}/job/gmall_report.ads_user_retention.json /warehouse/gmall/ads/ads_user_retention
  ;;
"ads_user_stats")
  export_data ${DATAX_HOME}/job/gmall_report.ads_user_stats.json /warehouse/gmall/ads/ads_user_stats
  ;;
"ads_activity_stats")
  export_data ${DATAX_HOME}/job/gmall_report.ads_activity_stats.json /warehouse/gmall/ads/ads_activity_stats
  ;;
"ads_coupon_stats")
  export_data ${DATAX_HOME}/job/gmall_report.ads_coupon_stats.json /warehouse/gmall/ads/ads_coupon_stats
  ;;
"ads_sku_cart_num_top3_by_cate")
  export_data ${DATAX_HOME}/job/gmall_report.ads_sku_cart_num_top3_by_cate.json /warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate
  ;;

"all")
  export_data ${DATAX_HOME}/job/gmall_report.ads_new_buyer_stats.json /warehouse/gmall/ads/ads_new_buyer_stats
  export_data ${DATAX_HOME}/job/gmall_report.ads_order_by_province.json /warehouse/gmall/ads/ads_order_by_province
  export_data ${DATAX_HOME}/job/gmall_report.ads_page_path.json /warehouse/gmall/ads/ads_page_path
  export_data ${DATAX_HOME}/job/gmall_report.ads_repeat_purchase_by_tm.json /warehouse/gmall/ads/ads_repeat_purchase_by_tm
  export_data ${DATAX_HOME}/job/gmall_report.ads_trade_stats.json /warehouse/gmall/ads/ads_trade_stats
  export_data ${DATAX_HOME}/job/gmall_report.ads_trade_stats_by_cate.json /warehouse/gmall/ads/ads_trade_stats_by_cate
  export_data ${DATAX_HOME}/job/gmall_report.ads_trade_stats_by_tm.json /warehouse/gmall/ads/ads_trade_stats_by_tm
  export_data ${DATAX_HOME}/job/gmall_report.ads_traffic_stats_by_channel.json /warehouse/gmall/ads/ads_traffic_stats_by_channel
  export_data ${DATAX_HOME}/job/gmall_report.ads_user_action.json /warehouse/gmall/ads/ads_user_action
  export_data ${DATAX_HOME}/job/gmall_report.ads_user_change.json /warehouse/gmall/ads/ads_user_change
  export_data ${DATAX_HOME}/job/gmall_report.ads_user_retention.json /warehouse/gmall/ads/ads_user_retention
  export_data ${DATAX_HOME}/job/gmall_report.ads_user_stats.json /warehouse/gmall/ads/ads_user_stats
  export_data ${DATAX_HOME}/job/gmall_report.ads_activity_stats.json /warehouse/gmall/ads/ads_activity_stats
  export_data ${DATAX_HOME}/job/gmall_report.ads_coupon_stats.json /warehouse/gmall/ads/ads_coupon_stats
  export_data ${DATAX_HOME}/job/gmall_report.ads_sku_cart_num_top3_by_cate.json /warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate
  ;;
esac
