package com.sky.ingestor.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.google.api.services.bigquery.model.TableRow;

public class RowMapperCloudfront implements RowMapper {

    private static final DateTimeFormatter DT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static Integer toInt(String s){ try{ return Integer.valueOf(s); }catch(Exception e){return null;} }
    private static Double  toDbl(String s){ try{ return Double.valueOf(s); }catch(Exception e){return null;} }
    private static Float   toFlt(String s){ try{ return Float.valueOf(s); }catch(Exception e){return null;} }

    private static String toDateTime(String date, String time){
        try {
            LocalDate ld = LocalDate.parse(date);      // "yyyy-MM-dd"
            LocalTime lt = LocalTime.parse(time);      // "HH:mm:ss"
            return DT.format(LocalDateTime.of(ld, lt));
        } catch(Exception e){
            return date + " " + time + ".000";
        }
    }

    @Override
    public TableRow toTableRow(Map<String, String> m) {
        String date = m.get("date");   // "2025-07-20"
        String time = m.get("time");   // "20:37:18"
        if (date == null || time == null) return null;

        TableRow t = new TableRow();

        t.set("request_id",       m.get("xEdgeRequestId"));           
        t.set("date_start",       date);                              
        t.set("request_time",     toDateTime(date, time));           
        t.set("client_ip",        m.get("csIp"));
        t.set("http_status_code", toInt(m.get("scStatus")));
        t.set("request_host",     firstNonEmpty(m.get("xHostHeader"), m.get("csHost")));
        t.set("request_path",     m.get("csUriStem"));
        t.set("response_content_type", m.get("scContentType"));
        t.set("user_agent",       m.get("csUserAgent"));

        Double totalBytes = toDbl(m.get("scBytes"));
        if (totalBytes == null) totalBytes = toDbl(m.get("csBytes"));
        t.set("total_bytes", totalBytes);

        t.set("turnaround_time",  toFlt(m.get("timeTaken")));         // seconds (float)
        t.set("transfer_time",    toFlt(m.get("timeToFirstByte")));   // seconds (float)

        t.set("error_code",                 m.get("xEdgeResultType")); 
        t.set("custom_field_unprocessed",   null);
        t.set("cache_status",               null);
        t.set("request_end_time",           null);
        t.set("asnum",                      null);
        t.set("breadcrumbs",                null);

        t.set("protocol_type",    firstNonEmpty(m.get("csProtocol"), m.get("csProtocolVersion"))); // "https" / "http/2.0"
        t.set("request_method",   m.get("csMethod"));
        t.set("cookie",           m.get("csCookie"));
        t.set("referer",          m.get("csReferer"));

        t.set("edge_ip",          m.get("xEdgeLocation"));

        t.set("xmt",  null);
        t.set("cmcd", null);
        t.set("crc",  null);
        t.set("phr",  null);

        return t;
    }

    private static String firstNonEmpty(String a, String b){
        if (a != null && !a.isEmpty()) return a;
        if (b != null && !b.isEmpty()) return b;
        return null;
    }
}
