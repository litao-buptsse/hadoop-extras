package com.sogou.hadoop.extras.monitor;

import com.sogou.hadoop.extras.common.CommonUtils;
import com.sun.org.glassfish.external.statistics.Statistic;
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
    NAMENODE_INFO.get(cluster).put(nameSpace2, new HashMap<>());
    NAMENODE_INFO.get(cluster).get(nameSpace2).put("nn3", "master03.sunshine.nm.ted");
    NAMENODE_INFO.get(cluster).get(nameSpace2).put("nn4", "master04.sunshine.nm.ted");
  }

  private static void statistics(String cluster) throws IOException {
    long time = System.currentTimeMillis();

    InfluxDB influxDB = InfluxDBFactory.connect(
        "http://rsync.litao.clouddev.sjs.ted:8086", "grafana", "123456");
    String dbName = cluster.toLowerCase() + "_hdfs";

    // 1. traverse ns
    for (Map.Entry<String, Map<String, String>> nsInfo : NAMENODE_INFO.get(cluster).entrySet()) {
      String ns = nsInfo.getKey();
      boolean isNSLevelCounted = false;

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

          if (name.equals("Hadoop:service=NameNode,name=FSNamesystem")) {
            if (isNSLevelCounted) {
              continue;
            }

            BatchPoints batchPoints = BatchPoints.database(dbName)
                .tag("namespace", ns)
                .retentionPolicy(RETERNTION_POLICY)
                .build();

            // 4. traverse field
            for (String field : new String[]{
                "CapacityTotalGB", "CapacityUsedGB", "CapacityRemainingGB",
                "FilesTotal", "BlocksTotal"
            }) {
              batchPoints.point(Point.measurement(field)
                  .time(time, TimeUnit.MILLISECONDS)
                  .addField("value", bean.getLong(field))
                  .build());
              System.out.println(String.format("%s %s %s %s %s: %s", cluster, ns, nn, domain, field, bean.getLong(field)));
            }
            influxDB.write(batchPoints);
          }
        }

        isNSLevelCounted = true;
      }
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    String cluster = args[0];
    while (true) {
      statistics(cluster);
      Thread.sleep(60 * 1000);
    }
  }
}