package com.sky.ingestor.utils;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.google.api.services.bigquery.model.TableRow;

public class RowMapperRaiway implements RowMapper {

    private static final DateTimeFormatter DT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                         .withZone(ZoneOffset.UTC);

    private static Integer toInt(String s){
        if (s == null || s.isBlank() || "-".equals(s)) return null;
        try { return Integer.valueOf(s.trim()); } catch(Exception e){ return null; }
    }
    private static Double toDbl(String s){
        if (s == null || s.isBlank() || "-".equals(s)) return null;
        try { return Double.valueOf(s.trim()); } catch(Exception e){ return null; }
    }
    private static Float toFlt(String s){
        if (s == null || s.isBlank() || "-".equals(s)) return null;
        try { return Float.valueOf(s.trim()); } catch(Exception e){ return null; }
    }

    private static String toDate(String isoZ){
        // "2024-12-08T19:26:11.495Z" -> "2024-12-08"
        return Instant.parse(isoZ).atZone(ZoneOffset.UTC).toLocalDate().toString();
    }
    private static String toDateTime(String isoZ){
        // "2024-12-08T19:26:11.495Z" -> "yyyy-MM-dd HH:mm:ss.SSS"
        return DT.format(Instant.parse(isoZ));
    }
    private static String pathOf(String url){
        if (url == null || url.isBlank() || "-".equals(url)) return url;
        try { return URI.create(url).getRawPath(); }
        catch(Exception e){ return url; }
    }
    private static String hostFromUrl(String url){
        if (url == null || url.isBlank() || "-".equals(url)) return null;
        try { return URI.create(url).getHost(); }
        catch(Exception e){ return null; }
    }

    @Override
    public TableRow toTableRow(Map<String,String> m){

        

        String isoTs = m.get("datetime");   // es. 2024-12-08T19:26:11.495Z
        if (isoTs == null || isoTs.isEmpty() || "-".equals(isoTs)) return null;

        String requestedHost = m.get("requestedHost");
        if (requestedHost == null || requestedHost.isBlank() || "-".equals(requestedHost)) {
            String h = hostFromUrl(m.get("csUri"));
            if (h != null && !h.isBlank()) {
                requestedHost = h;
            }
        }

        TableRow t = new TableRow();

        t.set("request_id", null);
        t.set("date_start",   toDate(isoTs));      // DATE ("yyyy-MM-dd")
        t.set("request_time", toDateTime(isoTs));  // DATETIME ("yyyy-MM-dd HH:mm:ss.SSS")
        t.set("client_ip", m.get("csIp"));
        Integer status = toInt(m.get("pssc"));
        if (status == null) status = toInt(m.get("scStatus"));
        t.set("http_status_code", status);
        t.set("request_host", requestedHost);
        t.set("request_path", pathOf(m.get("csUri")));          
        t.set("response_content_type", m.get("psct"));          
        t.set("user_agent", m.get("csUserAgent"));
        Double totalBytes = toDbl(m.get("responseSize"));
        if (totalBytes == null) totalBytes = toDbl(m.get("csBytes"));
        t.set("total_bytes", totalBytes);
        t.set("error_code", null);
        t.set("turnaround_time", toFlt(m.get("requestTime")));  
        t.set("transfer_time",   toFlt(m.get("ttfb")));         
        t.set("custom_field_unprocessed", null);
        t.set("cache_status",     null);
        t.set("request_end_time", null);
        t.set("asnum",            null);
        t.set("breadcrumbs",      null);
        t.set("protocol_type",  m.get("csProtocol"));
        t.set("request_method", m.get("httpMethod"));
        t.set("cookie",  null);
        t.set("referer", m.get("csReferer"));
        t.set("edge_ip", m.get("edge_ip"));
        t.set("xmt",  m.get("xmt"));
        t.set("cmcd", m.get("cmcd"));
        t.set("crc",  m.get("crc"));
        t.set("phr",  m.get("phr"));

        return t;
    }
}
