# 整体架构

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

![applog.jpg](https://cdn.nlark.com/yuque/0/2022/jpeg/2356284/1647854381019-c7c5f2b7-d088-4f5a-a18b-33aa0b90e438.jpeg#clientId=u4cff95c9-d4ac-4&from=drop&id=u17327fd7&margin=%5Bobject%20Object%5D&name=applog.jpg&originHeight=477&originWidth=1715&originalType=binary&ratio=1&size=78774&status=done&style=none&taskId=u27654fde-2522-4ee4-b4f5-2a9f9916511)

## 业务数据采集

![dblog.jpg](https://cdn.nlark.com/yuque/0/2022/jpeg/2356284/1647854381258-fe65d917-7ed9-4021-8935-bf01f17a51ea.jpeg#clientId=u4cff95c9-d4ac-4&from=drop&id=ue93a76cd&margin=%5Bobject%20Object%5D&name=dblog.jpg&originHeight=805&originWidth=1721&originalType=binary&ratio=1&size=123217&status=done&style=none&taskId=u1656bdd5-b90c-47ad-b580-aab2b16d7b8)

# 数据仓库
## 架构图

## 表

# 调度器

# 整体流程图
