package com.sky.ingestor.mig;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
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

    record Fields(
        String datetime,
        String csIp,
        String requestedHost,
        String csUri,
        String httpMethod,
        String pssc,
        String requestTime,
        String responseSize,
        String csBytes,
        String crc,
        String phr,
        String hii,
        String userAgent,
        String xmt,
        String psct,
        String cmcd,
        String ttfb,
        String protocol
    ) {}

    static Fields parse(String line) {
        String datetime = "";
        String csIp = "";
        String requestedHost = "";
        String csUri = "";
        String httpMethod = "";
        String pssc = "";
        String requestTime = "";
        String responseSize = "";
        String csBytes = "";
        String crc = "";
        String phr = "";
        String hii = "";
        String userAgent = "";
        String xmt = "";
        String psct = "";
        String cmcd = "";
        String ttfb = "";
        String protocol = "";

        int length = line.length();
        int pos = 0;
        int field = 0;

        while (field <= PROTOCOL && pos < length) {
            while (pos < length && isSeparator(line.charAt(pos))) pos++;
            if (pos >= length) break;

            int start = pos;
            while (pos < length && !isSeparator(line.charAt(pos))) pos++;

            switch (field) {
                case DATETIME -> datetime = line.substring(start, pos);
                case CS_IP -> csIp = line.substring(start, pos);
                case REQUESTED_HOST -> requestedHost = line.substring(start, pos);
                case CS_URI -> csUri = line.substring(start, pos);
                case HTTP_METHOD -> httpMethod = line.substring(start, pos);
                case PSSC -> pssc = line.substring(start, pos);
                case REQUEST_TIME -> requestTime = line.substring(start, pos);
                case RESPONSE_SIZE -> responseSize = line.substring(start, pos);
                case CS_BYTES -> csBytes = line.substring(start, pos);
                case CRC -> crc = line.substring(start, pos);
                case PHR -> phr = line.substring(start, pos);
                case HII -> hii = line.substring(start, pos);
                case USER_AGENT -> userAgent = line.substring(start, pos);
                case XMT -> xmt = line.substring(start, pos);
                case PSCT -> psct = line.substring(start, pos);
                case CMCD -> cmcd = line.substring(start, pos);
                case TTFB -> ttfb = line.substring(start, pos);
                case PROTOCOL -> protocol = line.substring(start, pos);
                default -> {
                    // Skip fields that are not used by routing or the BQ row mapper.
                }
            }
            field++;
        }

        return new Fields(
            datetime, csIp, requestedHost, csUri, httpMethod, pssc, requestTime,
            responseSize, csBytes, crc, phr, hii, userAgent, xmt, psct, cmcd,
            ttfb, protocol
        );
    }

    static String resolveService(Fields f) {
        String url = f.csUri();
        String host = f.requestedHost();
        if (host.isEmpty() || "-".equals(host)) host = hostFromUrl(url);

        String hostLower = host.toLowerCase(Locale.ROOT);
        String pathLower = url.toLowerCase(Locale.ROOT);

        boolean ads = hostLower.contains("ads") || pathLower.contains("ads");
        boolean live = hostLower.contains("lin") || hostLower.contains("live")
            || pathLower.contains("lin") || pathLower.contains("live")
            || pathLower.contains("/service/") || pathLower.contains("/channel");
        boolean vod = hostLower.contains("vod") || pathLower.contains(".nff") || pathLower.contains("vod");
        boolean ivod = hostLower.contains("ivod") || pathLower.contains("ivod");
        boolean npvr = hostLower.contains("npvr") || pathLower.contains("npvr");

        boolean skygo = hostLower.contains("skygo") || pathLower.contains("/100e/")
            || pathLower.contains("qgo") || hostLower.contains("skyq") || pathLower.contains("skyq");
        boolean nowtv = hostLower.contains("cssott02.com") || hostLower.contains("cdn13.skycdp.com")
            || pathLower.contains("cssott02.com") || pathLower.contains("cdn13.skycdp.com")
            || pathLower.contains("/016a/") || pathLower.contains("now");
        boolean vodstb = hostLower.contains("stb") || pathLower.contains("stb")
            || hostLower.contains("vod-stb") || hostLower.contains("pdl") || pathLower.contains("nff");
        boolean soip = hostLower.contains("cdn03.skycdp.com") || pathLower.contains("cdn03.skycdp.com")
            || hostLower.contains("c02.skycdp.com") || pathLower.contains("c02.skycdp.com");
        boolean hip = hostLower.contains("hip") || pathLower.contains("hip");
        boolean verdi = hostLower.contains("qpuck") || pathLower.contains("qpuck");

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
            case "nowtv_linear", "nowtv_vod", "skygo_linear", "skygo_vod" ->
                new Destination("it_skycdn_ott_logs", service);
            case "soip_linear", "soip_vod", "soip_ads", "soip_npvr", "soip_ivod" ->
                new Destination("it_skycdn_soip_logs", service);
            case "hip_linear", "hip_vod", "verdi_linear", "verdi_vod", "vod-stb" ->
                new Destination("it_skycdn_legacy_logs", service.replace('-', '_'));
            default -> null;
        };
    }

    static JSONObject toBigQueryJson(Fields f, String objectName, String dateInsert) {
        String isoTs = f.datetime();
        if (isoTs.isEmpty()) return null;

        Instant instant = Instant.parse(isoTs);
        JSONObject row = new JSONObject();
        put(row, "date_start", isoTs.length() >= 10 ? isoTs.substring(0, 10) : instant.atZone(ZoneOffset.UTC).toLocalDate().toString());
        put(row, "request_time", DT.format(instant));
        put(row, "client_ip", f.csIp());
        putNumber(row, "http_status_code", f.pssc(), true);
        put(row, "request_host", f.requestedHost());
        put(row, "request_path", pathOf(f.csUri()));
        put(row, "response_content_type", f.psct());
        put(row, "user_agent", f.userAgent());

        Double totalBytes = parseDouble(f.responseSize());
        if (totalBytes == null) totalBytes = parseDouble(f.csBytes());
        if (totalBytes != null) row.put("total_bytes", totalBytes);

        putNumber(row, "turnaround_time", f.requestTime(), false);
        putNumber(row, "transfer_time", f.ttfb(), false);
        put(row, "protocol_type", f.protocol());
        put(row, "request_method", f.httpMethod());
        put(row, "edge_ip", f.hii());
        put(row, "xmt", f.xmt());
        put(row, "cmcd", f.cmcd());
        put(row, "crc", f.crc());
        put(row, "phr", f.phr());
        put(row, "file_name", objectName);
        put(row, "date_insert", dateInsert);
        return row;
    }

    private static boolean isSeparator(char c) {
        return c <= ' ';
    }

    private static void put(JSONObject row, String key, String value) {
        if (value != null && !value.isEmpty()) row.put(key, value);
    }

    private static void putNumber(JSONObject row, String key, String value, boolean integer) {
        if (value == null || value.isEmpty()) return;
        try {
            if (integer) row.put(key, Integer.parseInt(value));
            else row.put(key, Double.parseDouble(value));
        } catch (Exception ignored) {
        }
    }

    private static Double parseDouble(String s) {
        try {
            return Double.valueOf(s);
        } catch (Exception e) {
            return null;
        }
    }

    private static String hostFromUrl(String url) {
        if (url == null || url.isEmpty()) return "";

        int start;
        int scheme = url.indexOf("://");
        if (scheme >= 0) {
            start = scheme + 3;
        } else if (url.startsWith("//")) {
            start = 2;
        } else {
            return "";
        }

        int end = url.length();
        for (int i = start; i < url.length(); i++) {
            char c = url.charAt(i);
            if (c == '/' || c == '?' || c == '#') {
                end = i;
                break;
            }
        }

        if (end <= start) return "";
        return url.substring(start, end);
    }

    private static String pathOf(String url) {
        if (url == null || url.isEmpty()) return url;

        int pathStart = 0;
        int scheme = url.indexOf("://");
        if (scheme >= 0) {
            int authorityStart = scheme + 3;
            int slash = url.indexOf('/', authorityStart);
            if (slash < 0) return "";
            pathStart = slash;
        } else if (url.startsWith("//")) {
            int slash = url.indexOf('/', 2);
            if (slash < 0) return "";
            pathStart = slash;
        }

        int end = url.length();
        int query = url.indexOf('?', pathStart);
        if (query >= 0) end = Math.min(end, query);
        int fragment = url.indexOf('#', pathStart);
        if (fragment >= 0) end = Math.min(end, fragment);

        return url.substring(pathStart, end);
    }
}
