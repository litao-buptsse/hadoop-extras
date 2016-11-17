# HDFS跨级群文件拷贝

## 背景

* 全量拷贝: 使用原生distcp,自带限速功能
* 增量拷贝: 使用自研工具,可递归拷贝文件、创建目录、设置权限时间等信息,并具限速功能

## 全量拷贝

```
./distcp_full <srcNN> <dstNN> <dir>
```

## 增量拷贝

```
./distcp_diff <srcNN> <dstNN> <dir>
```
```