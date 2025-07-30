package com.sky.ingestor.utils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.google.api.services.bigquery.model.TableRow;

public class RowMapperAkamai implements RowMapper {

    private static final DateTimeFormatter DT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                         .withZone(ZoneOffset.UTC);

    private static Integer toInt(String s){ try{ return Integer.valueOf(s); }catch(Exception e){return null;} }
    private static Double  toDbl(String s){ try{ return Double.valueOf(s); }catch(Exception e){return null;} }
    private static Float  toFlt(String s){ try{ return Float.valueOf(s); }catch(Exception e){return null;} }


    private static long   epochMs(String s){ return (long)(Double.parseDouble(s)*1000); }
    private static String toDate(String s){
        return Instant.ofEpochMilli(epochMs(s))
                      .atZone(ZoneOffset.UTC)
                      .toLocalDate()
                      .toString();          // "2025-07-10"
        }

    private static String toDateTime(String s){ return DT.format(Instant.ofEpochMilli(epochMs(s))); }

    @Override public TableRow toTableRow(Map<String,String> m){

        String epoch = m.getOrDefault("request_time", m.get("date_start"));
        if(epoch==null) return null;               

        TableRow t = new TableRow();
            t.set("stream_version",   toInt(m.get("stream_version")));
            t.set("cpcode",           toInt(m.getOrDefault("cpcode", m.get("cp_code"))));
            t.set("request_id",       m.get("request_id"));
            t.set("date_start",       toDate(epoch));       // DATE (partition)
            t.set("request_time",     toDateTime(epoch));   // DATETIME
            t.set("client_ip",        m.get("client_ip"));
            t.set("http_status_code", toInt(m.get("http_status_code")));
            t.set("request_host",     m.get("request_host"));
            t.set("request_path",     m.get("request_path"));
            t.set("response_content_type",
                  m.getOrDefault("response_content_type",
                                 m.get("response_content-type")));
            t.set("user_agent",
                  m.getOrDefault("user_agent", m.get("user-agent_*")));
            t.set("total_bytes",      toDbl(m.get("total_bytes")));
            t.set("error_code",       m.get("error_code"));
            t.set("turnaround_time",  toFlt(m.getOrDefault("turnaround_time",
                                                           m.get("turn_around_time"))));
            t.set("transfer_time",    toFlt(m.get("transfer_time")));
            t.set("dns_lookup_time",  m.get("dns_lookup_time"));

            t.set("custom_field_unprocessed", m.get("custom_field_*"));
            t.set("cache_status",     toInt(m.get("cache_status")));
            t.set("request_end_time", toFlt(m.get("request_end_time")));
            t.set("asnum",            toInt(m.get("asnum")));
            t.set("breadcrumbs",      m.get("breadcrumbs"));
            t.set("protocol_type",    m.get("protocol_type"));
            t.set("request_method",   m.get("request_method"));
            t.set("cookie",           m.get("cookie_*"));
            t.set("referer",          m.get("referer_*"));
            t.set("edge_ip",          m.get("edge_ip_*"));

        return t;    
    }
}
