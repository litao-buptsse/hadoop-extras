# HDFS Federation目录切分

## 第一步:全量拷贝(不停服务)

### 1. 生成拷贝文件列表

```
hadoop fs -ls -R /mydir > copylist.txt
```

### 2. 切分文件列表并上传HDFS

```
mkdir copylist; split -l 50000 copylist.txt copylist/mydir
hadoop fs -put copylist > /tmp
```

### 3. 根据文件列表将/mydir由ns1拷贝至ns2

```
hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.fastcp.DistributedFastCp \
  /tmp/copylist hdfs://ns1 hdfs://ns2 / /tmp/fastcp_result
```

### 4. 对拷贝至ns2的文件目录做chmod、chown

```
hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.fastcp.DistributedUpdateFileInfo
  /tmp/copylist hdfs://ns2 /tmp/updatefileinfo_result
```

## 第二步:增量拷贝(停服务)

### 1. 将ns1进入safemode,只读不可写

```
hadoop dfsadmin -safemode enter -Dfs.defaultFS=hdfs://ns1
```

### 2. 生成最新拷贝文件列表

```
hadoop fs -ls -R /mydir > copylist_new.txt
```

### 3. 最新文件列表与老文件求diff

```
bin/hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.hdfs.DiffFileList \
  copylist.txt copylist_new.txt > copylist_diff.txt
```

### 4. 切分diff文件列表并上传HDFS

```
mkdir copylist_diff; split -l 50000 copylist_diff.txt copylist_diff/mydir
hadoop fs -put copylist_diff > /tmp
```

### 5. 根据diff文件列表,将/mydir增量由ns1拷贝至ns2

```
hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.fastcp.DistributedFastCp \
  /tmp/copylist_diff hdfs://ns1 hdfs://ns2 / /tmp/fastcp_result_diff update
```

### 6. 对增量拷贝至ns2的文件目录做chmod、chown

```
hadoop jar hadoop-extras-1.0-SNAPSHOT.jar \
  com.sogou.hadoop.extras.tools.fastcp.DistributedUpdateFileInfo
  /tmp/copylist_diff hdfs://ns2 /tmp/updatefileinfo_result_diff
```

### 7. 将ns1离开safemode,切分完毕

```
hadoop dfsadmin -safemode leave -Dfs.defaultFS=hdfs://ns1
```