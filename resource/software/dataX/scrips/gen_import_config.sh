#!/bin/bash

DATAX_PATH='/home/zxy/software/datax/script'

python ${DATAX_PATH}/gen_import_config.py -d gmall -t activity_info
python ${DATAX_PATH}/gen_import_config.py -d gmall -t activity_rule
python ${DATAX_PATH}/gen_import_config.py -d gmall -t base_category1
python ${DATAX_PATH}/gen_import_config.py -d gmall -t base_category2
python ${DATAX_PATH}/gen_import_config.py -d gmall -t base_category3
python ${DATAX_PATH}/gen_import_config.py -d gmall -t base_dic
python ${DATAX_PATH}/gen_import_config.py -d gmall -t base_province
python ${DATAX_PATH}/gen_import_config.py -d gmall -t base_region
python ${DATAX_PATH}/gen_import_config.py -d gmall -t base_trademark
python ${DATAX_PATH}/gen_import_config.py -d gmall -t cart_info
python ${DATAX_PATH}/gen_import_config.py -d gmall -t coupon_info
python ${DATAX_PATH}/gen_import_config.py -d gmall -t sku_attr_value
python ${DATAX_PATH}/gen_import_config.py -d gmall -t sku_info
python ${DATAX_PATH}/gen_import_config.py -d gmall -t sku_sale_attr_value
python ${DATAX_PATH}/gen_import_config.py -d gmall -t spu_info
