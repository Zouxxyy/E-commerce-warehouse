`# coding=utf-8
import json
import getopt
import os
import sys
import pymysql

# MySQL相关配置，需根据实际情况作出修改
mysql_host = "dell-r720"
mysql_port = "3306"
mysql_user = "root"
mysql_passwd = "123456"

# HDFS NameNode相关配置，需根据实际情况作出修改
hdfs_nn_host = "dell-r720"
hdfs_nn_port = "8021"

# 生成配置文件的目标路径，可根据实际情况作出修改
output_path = "/home/zxy/software/datax/job/"


def get_connection():
    return pymysql.connect(host=mysql_host, port=int(mysql_port), user=mysql_user, passwd=mysql_passwd)


def get_mysql_meta(database, table):
    connection = get_connection()
    cursor = connection.cursor()
    sql = "SELECT COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION"
    cursor.execute(sql, [database, table])
    fetchall = cursor.fetchall()
    cursor.close()
    connection.close()
    return fetchall


def get_mysql_columns(database, table):
    return list(map(lambda x: x[0], get_mysql_meta(database, table)))


def generate_json(target_database, target_table):
    job = {
        "job": {
            "setting": {
                "speed": {
                    "channel": 3
                },
                "errorLimit": {
                    "record": 0,
                    "percentage": 0.02
                }
            },
            "content": [{
                "reader": {
                    "name": "hdfsreader",
                    "parameter": {
                        "path": "${exportdir}",
                        "defaultFS": "hdfs://" + hdfs_nn_host + ":" + hdfs_nn_port,
                        "column": ["*"],
                        "fileType": "text",
                        "encoding": "UTF-8",
                        "fieldDelimiter": "\t",
                        "nullFormat": "\\N"
                    }
                },
                "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "writeMode": "replace",
                        "username": mysql_user,
                        "password": mysql_passwd,
                        "column": get_mysql_columns(target_database, target_table),
                        "connection": [
                            {
                                "jdbcUrl":
                                    "jdbc:mysql://" + mysql_host + ":" + mysql_port + "/" + target_database + "?useUnicode=true&characterEncoding=utf-8",
                                "table": [target_table]
                            }
                        ]
                    }
                }
            }]
        }
    }
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    with open(os.path.join(output_path, ".".join([target_database, target_table, "json"])), "w") as f:
        json.dump(job, f)


def main(args):
    target_database = ""
    target_table = ""

    options, arguments = getopt.getopt(args, '-d:-t:', ['targetdb=', 'targettbl='])
    for opt_name, opt_value in options:
        if opt_name in ('-d', '--targetdb'):
            target_database = opt_value
        if opt_name in ('-t', '--targettbl'):
            target_table = opt_value

    generate_json(target_database, target_table)


if __name__ == '__main__':
    main(sys.argv[1:])
`