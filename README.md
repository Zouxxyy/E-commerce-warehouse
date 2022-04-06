# 整体架构

![architecture](https://raw.githubusercontent.com/Zouxxyy/E-commerce-warehouse/master/resource/images/architecture.png)

## 集群服务规划

| 服务名称             | 子服务               | 服务器 dell-r720 | 服务器 dell-r730-4 | 服务器 dell-r730-5 |
|------------------|-------------------|---------------|-----------------|-----------------|
| HDFS             | NameNode          | ✓             |                 |                 |
|                  | DataNode          | ✓             | ✓               | ✓               |
|                  | SecondaryNameNode |               |                 | ✓               |
| YARN             | Resourcemanager   |               | ✓               |                 |
|                  | NodeManager       | ✓             | ✓               | ✓               |
| Zookeeper        | Zookeeper Server  | ✓             | ✓               | ✓               |
| Flume（采集）        | Flume             | ✓             | ✓               |                 |
| Kafka            | Kafka             | ✓             | ✓               | ✓               |
| Flume（消费）        | Flume             |               |                 | ✓               |
| MySQL            | MySQL             | ✓             |                 |                 |
| DataX            | DataX             | ✓             | ✓               | ✓               |
| Maxwell          | Maxwell           | ✓             |                 |                 |
| Hive             | -                 | ✓             | ✓               | ✓               |
| Spark            | -                 | ✓             | ✓               | ✓               |
| DolphinScheduler | MasterServer      | ✓             |                 |                 |
|                  | WorkerServer      | ✓             | ✓               | ✓               |


## jpsall效果

```shell
--------- dell-r720 ----------
5696 JobHistoryServer
4965 NameNode
22533 AlertServer
22629 ApiApplicationServer
1864 NodeManager
5100 DataNode
9487 QuorumPeerMain
20273 Kafka
22289 MasterServer
22386 WorkerServer
25977 RunJar
26363 Application (flume1)
22460 LoggerServer
28287 Maxwell
--------- dell-r730-4 ----------
51072 LoggerServer
3556 DataNode
8276 NodeManager
15205 QuorumPeerMain
13530 Application
17131 Kafka
50942 WorkerServer
8078 ResourceManager
--------- dell-r730-5 ----------
40624 NodeManager
33297 Kafka
21780 QuorumPeerMain
30196 Application (flume2)
28997 WorkerServer
20840 Application (flume3)
29112 LoggerServer
38361 SecondaryNameNode
38172 DataNode
```

# 数据采集

## 用户行为日志采集

![applog](https://raw.githubusercontent.com/Zouxxyy/E-commerce-warehouse/master/resource/images/applog.jpg)

## 业务数据采集

![dblog](https://raw.githubusercontent.com/Zouxxyy/E-commerce-warehouse/master/resource/images/dblog.jpg)

# 数据仓库
## 架构图

![warehouse](https://raw.githubusercontent.com/Zouxxyy/E-commerce-warehouse/master/resource/images/warehouse.jpg)

## 表

![tables](https://raw.githubusercontent.com/Zouxxyy/E-commerce-warehouse/master/resource/images/tables.jpg)

# 调度器

![dolphinscheduler](https://raw.githubusercontent.com/Zouxxyy/E-commerce-warehouse/master/resource/images/dolphinscheduler.jpg)

# 整体流程图
