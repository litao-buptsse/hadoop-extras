package com.sogou.hadoop.extras.common;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Tao Li on 2016/8/24.
 */
public class CommonUtils {
  public static String readAll(Reader rd) throws IOException {
    StringBuilder sb = new StringBuilder();
    int cp;
    while ((cp = rd.read()) != -1) {
      sb.append((char) cp);
    }
    return sb.toString();
  }

  public static JSONObject readJSONObject(String url) throws IOException {
    try (BufferedReader rd = new BufferedReader(new InputStreamReader(new URL(url).openStream()))) {
      String jsonText = readAll(rd);
      return new JSONObject(jsonText);
    }
  }

  public static double percentageStringToDouble(String percentage) {
    return new BigDecimal(percentage.trim().replace("%", ""))
        .divide(BigDecimal.valueOf(100)).doubleValue();
  }

  public static String now(String pattern) {
    return new SimpleDateFormat(pattern).format(new Date(System.currentTimeMillis()));
  }

  public static void main(String[] args) {
    System.out.println(now("yyyyMMdd"));
  }
}
