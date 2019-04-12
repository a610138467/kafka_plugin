# Kafka Plugin

## 说明

将eos的数据导入到kafka集群,该plugin只负责导入原始数据，具体的输出处理应该由Kafak Stream进一步完成

## 依赖

**libkafka**

## 编译

### 安装libkafka

```
git clone https://github.com/edenhill/librdkafka
cd librdkafka
cmake -H. -B_cmake_build
cmake -DRDKAFKA_BUILD_STATIC=1 --build _cmake_build
cd _cmake_build
make install
```

### 下载kafka-plugin代码到eosio的plugs目录

```
cd plugins
git clone https://github.com/a610138467/kafka_plugin.gitz
cd ..
```

### 初始化环境

```
./plugins/kafka_plugin/init.sh
```

### 编译eosio的代码

```
./eosio_build.sh
```

## 使用

```
./build/programs/nodeos/nodeos --plugin eosio::kafka_plugin --kafka-enable true --kafka-broker-list ${kafkanode1},${kafkanode2}......
```

## 支持参数

参数名称 | 参数说明
------ | --------
kafka-plugin-enable | 是否启动
kafka-plugin-broker | kafka地址，可以提供多个
kafka-plugin-enable-accepted-block-connection|是否允许从accepted\_block回调获取数据导入到kafka
kafka-plugin-accepted-block-topic-name|如果允许,accepted\_block回调的数据导入到哪个topic中
kafka-plugin-enable-irreversible-block-connection|是否允许从irreversible\_block回调获取数据导入到kafka
kafka-plugin-irreversible-block-topic-name|如果允许,irreversible\_block回调的数据导入到哪个topic中
kafka-plugin-enable-applied-transaction-connection|是否允许从applied\_transaction回调获取数据导入到kafka
kafka-plugin-applied-transaction-topic-name|如果允许,applied\_transaction回调获取的数据导入到哪个topic中
kafka-plugin-enable-accepted-transaction-connection|是否允许从accepted\_transaction回调获取数据导入到kafka中
kafka-plugin-accepted-transaction-topic-name|如果允许,accepted\_transaction回调获取的数据导入到哪个topic中
