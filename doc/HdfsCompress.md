# HDFS文件压缩工具

## 背景

* 支持小文件合并
* 支持热数据（lzozx-1）、温数据（lzozx-999）、冷数据（bzip2）压缩

## 使用方式

```
./compress_hdfs <inputPath> <outputPath> [HOT|WARM|COLD]
```