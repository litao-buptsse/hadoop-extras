# HDFS文件快速下载工具

## 背景

使用多线程对HDFS文件进行分块下载,大幅提升下载速度,并具限速功能

## 使用方式
```
./fastget <hdfsSrc> <localDst> [threadNum] [blockSize] [bandWidth]
```