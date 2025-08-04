package com.sky.ingestor.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.google.api.services.bigquery.model.TableRow;

public class RowMapperCloudfrontLegacy implements RowMapper {

    private static final DateTimeFormatter DT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static Integer toInt(String s){
        try { return (s == null || s.isBlank() || "-".equals(s)) ? null : Integer.valueOf(s.trim()); }
        catch(Exception e){ return null; }
    }
    private static Double toDbl(String s){
        try { return (s == null || s.isBlank() || "-".equals(s)) ? null : Double.valueOf(s.trim()); }
        catch(Exception e){ return null; }
    }
    private static Float toFlt(String s){
        try { return (s == null || s.isBlank() || "-".equals(s)) ? null : Float.valueOf(s.trim()); }
        catch(Exception e){ return null; }
    }

    private static String toDateTime(String date, String time){
        try {
            LocalDate ld = LocalDate.parse(date);      // "yyyy-MM-dd"
            LocalTime lt = LocalTime.parse(time);      // "HH:mm:ss"
            return DT.format(LocalDateTime.of(ld, lt));
        } catch(Exception e){
            // fallback tollerante
            String d = (date == null ? "" : date.trim());
            String t = (time == null ? "" : time.trim());
            if (!d.isEmpty() && !t.isEmpty()) return d + " " + t + ".000";
            return null;
        }
    }

    @Override
    public TableRow toTableRow(Map<String, String> m) {
        String date = m.get("date");   // "yyyy-MM-dd"
        String time = m.get("time");   // "HH:mm:ss"
        if (date == null || time == null) return null;

        TableRow t = new TableRow();

        // campi chiave
        t.set("request_id",       null);                         // non presente nel legacy
        t.set("date_start",       date);                         // DATE string "yyyy-MM-dd"
        t.set("request_time",     toDateTime(date, time));       // DATETIME string "yyyy-MM-dd HH:mm:ss.SSS"

        // base
        t.set("client_ip",        m.get("csIp"));
        t.set("http_status_code", toInt(m.get("scStatus")));
        t.set("request_host",     m.get("xHostHeader"));         // niente csHost nel legacy
        t.set("request_path",     m.get("csUriStem"));
        t.set("response_content_type", null);                    // non presente nel legacy
        t.set("user_agent",       m.get("csUserAgent"));

        // dimensioni e tempi
        t.set("total_bytes",      toDbl(m.get("scBytes")));
        t.set("turnaround_time",  toFlt(m.get("timeTaken")));    // seconds
        t.set("transfer_time",    null);                         // timeToFirstByte non presente

        // vari
        t.set("error_code",                null);                // xEdgeResultType non presente
        t.set("custom_field_unprocessed",  m.get("smart_card")); // preservo smart_card qui; metti null se non lo vuoi
        t.set("cache_status",              null);                // ATTENZIONE: schema INTEGER; il legacy ha stringhe (es. TCP_HIT)
        t.set("request_end_time",          null);
        t.set("asnum",                     null);
        t.set("breadcrumbs",               null);

        t.set("protocol_type",   null);                          // non c'è csProtocol/csProtocolVersion
        t.set("request_method",  m.get("csMethod"));
        t.set("cookie",          m.get("csCookie"));
        t.set("referer",         m.get("csReferer"));

        t.set("edge_ip",         m.get("xEdgeLocation"));        // è una location, ma coerente col mapper CF “normale”
        t.set("xmt",  null);
        t.set("cmcd", null);
        t.set("crc",  null);
        t.set("phr",  null);

        return t;
    }
}
