# HDFS Federation目录切分

## 背景

* HDFS Federation: 对于HDFS集群,随着数据规模扩大,单NameNode因托管大量元信息成为瓶颈。可拆分多个NameNode,分别托管不同目录,降低单NameNode的压力。
* Facebook FastCopy: 对于同集群内跨NameNode数据拷贝,可使用HardLink替换原生Copy,大幅降低网络磁盘IO。
* 对于已有HDFS集群进行Federation拆分时,使用FastCopy替换原有Distcp,无需大规模数据数据,大幅缩短迁移时间

## 第一步:全量拷贝(不停服务)

```
./fastcp_full <srcNN> <dstNN> <dir>
```

## 第二步:增量拷贝(停服务)

### 1. 源NN进safemode

```
hadoop dfsadmin -safemode enter -Dfs.defaultFS=<srcNN>
```

### 2. 增量FastCopy

```
./fastcp_diff <srcNN> <dstNN> <dir>
```


### 3. ns1离开safemode

```
hadoop dfsadmin -safemode leave -Dfs.defaultFS=<srcNN>
```

## 第三步:删除源NN托管目录

```
./fastcp_delete_src <srcNN> <dir>
```