package com.sky.ingestor;

import java.util.*;


public class ServiceResolver {
    public static String resolveService(Map<String, String> row) {

        ///////////
        // detect file type from ModelResolver. If AWS, use xHostHeader or csUriStem; if Akamai, use request_host or request_path; etc
        ///////////
        

        String model = row.getOrDefault("_model", "");
        String host;
        String path;

        switch (model) {
          case "akamai":
              host = row.getOrDefault("request_host", "").toLowerCase();
              path = row.getOrDefault("request_path", "").toLowerCase();
              break;
          case "cloudfront":
              host = row.getOrDefault("xHostHeader", "").toLowerCase();
              path = row.getOrDefault("csUriStem", "").toLowerCase();
              break;
          case "raiway":
              host = row.getOrDefault("xHostHeader", "").toLowerCase();
              path = row.getOrDefault("csUriStem", "").toLowerCase();
              break;
          default:
              host = "";
              path = "";
        }



        if (host.contains("cdn13.skycdp.com") || host.contains("cssott02.com")) {
            if (host.contains("vod")) return "nowtv_vod_";
            if (host.contains("lin")) return "nowtv_linear";
        } else if (host.contains("cdn03.skycdp.com")) {
            if (host.contains("vod")) return "soip_vod_";
            if (host.contains("lin")) return "soip_linear";
        } else if (host.contains("c02.skycdp.com")) {
            if (host.contains("vod")) return "soip_vod_";
            if (host.contains("lin")) return "soip_linear";
        } else if (host.contains("sit-vod-stb")) {
            return "soip_vod_";
        } else if (host.contains("linear")) {
            return "nowtv_linear_";
        }
        return "nomatch";
    }
}