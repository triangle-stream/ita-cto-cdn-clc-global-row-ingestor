package com.sky.ingestor.mig;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.json.JSONObject;

final class SkyCdnParser {
    static final int DATETIME = 0;
    static final int CS_IP = 1;
    static final int REQUESTED_HOST = 4;
    static final int CS_URI = 5;
    static final int HTTP_METHOD = 6;
    static final int PSSC = 7;
    static final int REQUEST_TIME = 8;
    static final int RESPONSE_SIZE = 9;
    static final int CS_BYTES = 11;
    static final int CRC = 14;
    static final int PHR = 15;
    static final int HII = 18;
    static final int USER_AGENT = 19;
    static final int XMT = 20;
    static final int PSCT = 23;
    static final int CMCD = 26;
    static final int TTFB = 27;
    static final int PROTOCOL = 28;

    private static final DateTimeFormatter DT = DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    record Destination(String dataset, String table) {}

    static String[] split(String line) {
        return line.trim().split("\\s+");
    }

    static String resolveService(String[] f) {
        String url = get(f, CS_URI);
        String host = get(f, REQUESTED_HOST).toLowerCase();
        if (host.isEmpty() || "-".equals(host)) host = hostFromUrl(url);
        String path = url.toLowerCase();

        boolean ads = host.contains("ads") || path.contains("ads");
        boolean live = host.contains("lin") || host.contains("live") || path.contains("lin") || path.contains("live") || path.contains("/service/") || path.contains("/channel");
        boolean vod = host.contains("vod") || path.contains(".nff") || path.contains("vod");
        boolean ivod = host.contains("ivod") || path.contains("ivod");
        boolean npvr = host.contains("npvr") || path.contains("npvr");

        boolean skygo = host.contains("skygo") || path.contains("/100e/") || path.contains("qgo") || host.contains("skyq") || path.contains("skyq");
        boolean nowtv = host.contains("cssott02.com") || host.contains("cdn13.skycdp.com") || path.contains("cssott02.com") || path.contains("cdn13.skycdp.com") || path.contains("/016a/") || path.contains("now");
        boolean vodstb = host.contains("stb") || path.contains("stb") || host.contains("vod-stb") || host.contains("pdl") || path.contains("nff");
        boolean soip = host.contains("cdn03.skycdp.com") || path.contains("cdn03.skycdp.com") || host.contains("c02.skycdp.com") || path.contains("c02.skycdp.com");
        boolean hip = host.contains("hip") || path.contains("hip");
        boolean verdi = host.contains("qpuck") || path.contains("qpuck");

        if (skygo) { if (live) return "skygo_linear"; if (vod) return "skygo_vod"; }
        if (nowtv) { if (live) return "nowtv_linear"; if (vod) return "nowtv_vod"; }
        if (vodstb) return "vod_stb";
        if (hip) { if (live) return "hip_linear"; if (vod) return "hip_vod"; }
        if (verdi) { if (live) return "verdi_linear"; if (vod) return "verdi_vod"; }
        if (soip) {
            if (live) return "soip_linear";
            if (ivod) return "soip_ivod";
            if (vod) return "soip_vod";
            if (ads) return "soip_ads";
            if (npvr) return "npvr";
        }
        return "nomatch";
    }

    static Destination destination(String service) {
        return switch (service) {
            case "nowtv_linear", "nowtv_vod", "skygo_linear", "skygo_vod" -> new Destination("it_skycdn_ott_logs", service);
            case "soip_linear", "soip_vod", "soip_ads", "soip_npvr", "soip_ivod" -> new Destination("it_skycdn_soip_logs", service);
            case "hip_linear", "hip_vod", "verdi_linear", "verdi_vod", "vod-stb" -> new Destination("it_skycdn_legacy_logs", service.replace('-', '_'));
            default -> null;
        };
    }

    static JSONObject toBigQueryJson(String[] f, String objectName, String dateInsert) {
        String isoTs = get(f, DATETIME);
        if (isoTs.isEmpty()) return null;

        Instant instant = Instant.parse(isoTs);
        JSONObject row = new JSONObject();
        put(row, "date_start", instant.atZone(ZoneOffset.UTC).toLocalDate().toString());
        put(row, "request_time", DT.format(instant));
        put(row, "client_ip", get(f, CS_IP));
        putNumber(row, "http_status_code", get(f, PSSC), true);
        put(row, "request_host", get(f, REQUESTED_HOST));
        put(row, "request_path", pathOf(get(f, CS_URI)));
        put(row, "response_content_type", get(f, PSCT));
        put(row, "user_agent", get(f, USER_AGENT));

        Double totalBytes = parseDouble(get(f, RESPONSE_SIZE));
        if (totalBytes == null) totalBytes = parseDouble(get(f, CS_BYTES));
        if (totalBytes != null) row.put("total_bytes", totalBytes);

        putNumber(row, "turnaround_time", get(f, REQUEST_TIME), false);
        putNumber(row, "transfer_time", get(f, TTFB), false);
        put(row, "protocol_type", get(f, PROTOCOL));
        put(row, "request_method", get(f, HTTP_METHOD));
        put(row, "edge_ip", get(f, HII));
        put(row, "xmt", get(f, XMT));
        put(row, "cmcd", get(f, CMCD));
        put(row, "crc", get(f, CRC));
        put(row, "phr", get(f, PHR));
        put(row, "file_name", objectName);
        put(row, "date_insert", dateInsert);
        return row;
    }

    private static String get(String[] f, int i) { return i < f.length ? f[i] : ""; }
    private static void put(JSONObject row, String key, String value) { if (value != null && !value.isEmpty()) row.put(key, value); }
    private static void putNumber(JSONObject row, String key, String value, boolean integer) {
        if (value == null || value.isEmpty()) return;
        try { if (integer) row.put(key, Integer.parseInt(value)); else row.put(key, Double.parseDouble(value)); } catch (Exception ignored) {}
    }
    private static Double parseDouble(String s) { try { return Double.valueOf(s); } catch (Exception e) { return null; } }
    private static String hostFromUrl(String url) {
        try { String h = URI.create(url).getHost(); return h == null ? "" : h.toLowerCase(); } catch (Exception e) { return ""; }
    }
    private static String pathOf(String url) {
        try { String p = URI.create(url).getRawPath(); return p == null ? url : p; } catch (Exception e) { return url; }
    }
}
