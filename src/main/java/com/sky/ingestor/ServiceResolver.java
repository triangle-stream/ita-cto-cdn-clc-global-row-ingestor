package com.sky.ingestor;

import java.net.URI;
import java.util.Map;


public class ServiceResolver {

    // Helpers 

    private static String lc(Map<String,String> m, String k){
        return m.getOrDefault(k, "").toLowerCase();
    }

    // Helper: extract host from URL, if the host field is empty 
    private static String hostFromUrl(String url){
    if (url == null || url.isEmpty()) return "";
    try {
        String h = URI.create(url).getHost();
        return h == null ? "" : h.toLowerCase();
    } catch (Exception e) {
        return "";
    }
}

    //* Extract host and path fields based on models 
    private static String[] hostPath(Map<String,String> row){
        switch (row.getOrDefault("_model", "")) {
            case "akamai":
                return new String[]{ lc(row,"request_host"),
                                     lc(row,"request_path") };
            case "cloudfront":
                return new String[]{ lc(row,"xHostHeader"),
                                     lc(row,"csUriStem")   };
            case "cloudfront_legacy":
                return new String[]{ lc(row,"xHostHeader"),
                                     lc(row,"csUriStem")   };
            case "raiway": {
                String url  = row.getOrDefault("csUri", "");
                String host = lc(row, "requestedHost");
                if (host.isEmpty() || "-".equals(host)) {
                    host = hostFromUrl(url); // già lowercase
                }
                String path = lc(row, "csUriStem");
                if (path.isEmpty()) {
                    path = url.toLowerCase();
                }
                return new String[]{ host, path };
        }
            case "skycdn": {
                String url  = row.getOrDefault("csUri", "");
                String host = lc(row, "requestedHost");
                if (host.isEmpty() || "-".equals(host)) {
                    host = hostFromUrl(url);
                }
                return new String[]{ host, url.toLowerCase() };
            }
            default:
                return new String[]{"",""};
        }
    }

    // Check if it's linear, vod, etc. 
    private static boolean isAds(String host,String path){
        return host.contains("ads")   || path.contains("ads");
    }
    private static boolean isLive(String host, String path){
        return host.contains("lin")               ||
               host.contains("live")               ||
               path.contains("lin")               ||
               path.contains("live")              ||
               path.contains("/service/")             ||
               path.contains("/channel");
    }
    private static boolean isVod(String host, String path){
        return host.contains("vod")              ||
               path.contains(".nff")             ||
               path.contains("vod");
    }

    private static boolean isIvod(String host,String path){
        return host.contains("ivod")   || path.contains("ivod");
    }
    private static boolean isNpvr(String host,String path){
        return host.contains("npvr")   || path.contains("npvr");
    }

    // Check the service based on host and/or path 
    private static boolean isSkyGo(String host,String path){
        return host.contains("skygo") || path.contains("/100e/") || path.contains("qgo") ||
        host.contains("skyq") || path.contains("skyq");
    }
    private static boolean isNowTV(String host, String path){
        return host.contains("cssott02.com") || 
               host.contains("cdn13.skycdp.com")
            || path.contains("cssott02.com") || path.contains("cdn13.skycdp.com")
            || path.contains("/016a/") || path.contains("now");
    }
    /// VODSTB before Soip cause sometimes they share the host c02.skycdp.com
    private static boolean isVodStb(String host,String path){        
        return host.contains("stb") || path.contains("stb") || host.contains("vod-stb") || host.contains("pdl") || path.contains("nff");
    }    
    private static boolean isSoip(String host,String path){
        return host.contains("cdn03.skycdp.com") || path.contains("cdn03.skycdp.com") ||
        host.contains("c02.skycdp.com") || path.contains("c02.skycdp.com");
    }
    private static boolean isHip(String host,String path){
        return host.contains("hip")   || path.contains("hip");
    }
    private static boolean isVerdi(String host,String path){        
        return host.contains("qpuck") || path.contains("qpuck");
    }


    public static String resolveService(Map<String,String> row){

        String[] hp   = hostPath(row);
        String   host = hp[0];
        String   path = hp[1];

        boolean live = isLive(host,path);
        boolean vodstb = isVodStb(host,path);
        boolean vod  = isVod(host,path);
        boolean ads = isAds(host,path);
        boolean ivod = isIvod(host,path);
        boolean npvr = isNpvr(host,path);

        // SKY GO ------------------------------------------------------------ 
        if (isSkyGo(host,path)){
            if (live) return "skygo_linear";
            if (vod)  return "skygo_vod";
        }
        // NOW TV ------------------------------------------------------------ 
        if (isNowTV(host,path)){
            if (live) return "nowtv_linear";
            if (vod)  return "nowtv_vod";
        }


        // VOD-STB ----------------------------------------------------- 
        if (isVodStb(host,path)){
            if (vodstb)  return "vod_stb";
        }                

        // HIP (IPTV) -------------------------------------------------------- 
        if (isHip(host,path)){
            if (live) return "hip_linear";
            if (vod)  return "hip_vod";
        }

        // VERDI / QPUCK ----------------------------------------------------- 
        if (isVerdi(host,path)){
            if (live) return "verdi_linear";
            if (vod)  return "verdi_vod";
        }

         // SOIP (Glass/Stream) ------------------------------------------------------------ 
        if (isSoip(host,path)){
            if (live) return "soip_linear";
            if (ivod) return "soip_ivod";
            if (vod)  return "soip_vod";
            if (ads)  return "soip_ads";
            if (npvr) return "npvr";
        }

        // nessuna corrispondenza 
        return "nomatch";
    }
}