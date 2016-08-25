package com.sogou.hadoop.extras.monitor;

import com.sogou.hadoop.extras.common.CommonUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Tao Li on 2016/8/24.
 */
public class HDFSMonitor {
  private static Map<String, Map<String, Map<String, String>>> NAMENODE_INFO = new HashMap<>();
  private final static String RETERNTION_POLICY = "two_month";

  static {
    String cluster = "Sunshine";
    String nameSpace1 = "SunshineNameNode";
    String nameSpace2 = "SunshineNameNode2";
    NAMENODE_INFO.put(cluster, new HashMap<>());
    NAMENODE_INFO.get(cluster).put(nameSpace1, new HashMap<>());
    NAMENODE_INFO.get(cluster).get(nameSpace1).put("nn1", "master01.sunshine.nm.ted");
    NAMENODE_INFO.get(cluster).get(nameSpace1).put("nn2", "master02.sunshine.nm.ted");
    // NAMENODE_INFO.get(cluster).put(nameSpace2, new HashMap<>());
    // NAMENODE_INFO.get(cluster).get(nameSpace2).put("nn3", "master03.sunshine.nm.ted");
    // NAMENODE_INFO.get(cluster).get(nameSpace2).put("nn4", "master04.sunshine.nm.ted");
  }

  enum DataType {
    LONG, DOUBLE, PERCENTAGE_STRING
  }

  private static void statistics(String cluster) throws IOException {
    long time = System.currentTimeMillis();

    InfluxDB influxDB = InfluxDBFactory.connect(
        "http://rsync.litao.clouddev.sjs.ted:8086", "grafana", "123456");
    String dbName = cluster.toLowerCase() + "_hdfs";

    // 1. traverse ns
    for (Map.Entry<String, Map<String, String>> nsInfo : NAMENODE_INFO.get(cluster).entrySet()) {
      String ns = nsInfo.getKey();

      BatchPoints batchPoints = BatchPoints.database(dbName)
          .tag("namespace", ns)
          .retentionPolicy(RETERNTION_POLICY)
          .build();

      // 2. traverse nn
      for (Map.Entry<String, String> nnInfo : nsInfo.getValue().entrySet()) {
        String nn = nnInfo.getKey();
        String domain = nnInfo.getValue();

        String url = String.format("http://%s:50070/jmx", domain);
        JSONObject json = CommonUtils.readJSONObject(url);
        JSONArray beans = json.getJSONArray("beans");

        // 3. traverse beans
        for (int i = 0; i < beans.length(); i++) {
          JSONObject bean = beans.getJSONObject(i);
          String name = bean.getString("name");

          switch (name) {
            case "Hadoop:service=NameNode,name=FSNamesystem":
              if (bean.getString("tag.HAState").equals("standby")) {
                break;
              }
              updateBatchPoints(batchPoints, bean, new String[]{
                  "CapacityTotal", "CapacityUsed", "CapacityRemaining",
                  "FilesTotal", "BlocksTotal",
                  "CorruptBlocks", "MissingBlocks", "UnderReplicatedBlocks", "PendingReplicationBlocks", "PendingDeletionBlocks"
              }, time, DataType.LONG);
              break;
            case "Hadoop:service=NameNode,name=FSNamesystemState":
              updateBatchPoints(batchPoints, bean, new String[]{
                  "NumLiveDataNodes", "NumDeadDataNodes", "NumDecommissioningDataNodes", "VolumeFailuresTotal"
              }, time, DataType.LONG);
              break;
            case "Hadoop:service=NameNode,name=NameNodeInfo":
              JSONObject nodeUsage = new JSONObject(bean.getString("NodeUsage")).getJSONObject("nodeUsage");
              updateBatchPoints(batchPoints, nodeUsage, new String[]{"min", "median", "max", "stdDev"}, time, DataType.PERCENTAGE_STRING);

              JSONObject liveNodes = new JSONObject(bean.getString("LiveNodes"));
              long totalLastContact = 0;
              long maxLastContact = 0;
              long minLastContact = 0;
              int dnNum = 0;
              for (String dn : liveNodes.keySet()) {
                JSONObject dnInfo = liveNodes.getJSONObject(dn);
                long lastContact = dnInfo.getLong("lastContact");
                totalLastContact += lastContact;
                dnNum++;
                maxLastContact = lastContact > maxLastContact ? lastContact : maxLastContact;
                minLastContact = lastContact < minLastContact ? lastContact : minLastContact;
              }
              long avgLastContact = totalLastContact / dnNum;
              JSONObject lastContactJson = new JSONObject();
              lastContactJson.put("maxLastContact", maxLastContact);
              lastContactJson.put("minLastContact", minLastContact);
              lastContactJson.put("avgLastContact", avgLastContact);
              updateBatchPoints(batchPoints, lastContactJson, new String[]{"maxLastContact", "minLastContact", "avgLastContact"}, time, DataType.LONG);
              break;
            case "Hadoop:service=NameNode,name=JvmMetrics":
              updateBatchPoints(batchPoints, bean, new String[]{
                  "GcCountConcurrentMarkSweep", "GcTimeMillisConcurrentMarkSweep"
              }, time, DataType.LONG);
              break;
          }
        }
      }

      influxDB.write(batchPoints);
    }
  }

  private static <T extends Number> void updateBatchPoints(BatchPoints batchPoints, JSONObject bean,
                                                           String[] fields, long time, DataType type) {
    for (String field : fields) {
      Number value = null;
      switch (type) {
        case LONG:
          value = bean.getLong(field);
          break;
        case DOUBLE:
          value = bean.getDouble(field);
          break;
        case PERCENTAGE_STRING:
          value = CommonUtils.percentageStringToDouble(bean.getString(field));
          break;
      }
      batchPoints.point(Point.measurement(field)
          .time(time, TimeUnit.MILLISECONDS)
          .addField("value", value)
          .build());
      System.out.println(String.format("%s %s: %s", time, field, value));
    }
  }

  public static void main(String[] args) throws InterruptedException {
    String cluster = args[0];
    while (true) {
      try {
        statistics(cluster);
      } catch (IOException e) {
        e.printStackTrace();
      }
      Thread.sleep(5 * 60 * 1000);
    }
  }
}