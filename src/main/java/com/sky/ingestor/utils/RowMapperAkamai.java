package com.sky.ingestor.utils;

import com.google.api.services.bigquery.model.TableRow;
import java.sql.Date;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class RowMapperAkamai implements RowMapper {

    private static final DateTimeFormatter DT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                         .withZone(ZoneOffset.UTC);

    private static Integer toInt(String s){ try{ return Integer.valueOf(s); }catch(Exception e){return null;} }
    private static Double  toDbl(String s){ try{ return Double.valueOf(s); }catch(Exception e){return null;} }

    private static long   epochMs(String s){ return (long)(Double.parseDouble(s)*1000); }
    private static Date   toDate(String s){ return Date.valueOf(Instant.ofEpochMilli(epochMs(s)).atZone(ZoneOffset.UTC).toLocalDate()); }
    private static String toDateTime(String s){ return DT.format(Instant.ofEpochMilli(epochMs(s))); }

    @Override public TableRow toTableRow(Map<String,String> m){

        String epoch = m.getOrDefault("request_time", m.get("date_start"));
        if(epoch==null) return null;                // riga incompleta → scartata a monte

        TableRow t = new TableRow();
        t.set("stream_version",   toInt(m.get("stream_version")));
        t.set("cpcode",           toInt(m.getOrDefault("cpcode", m.get("cp_code"))));
        t.set("request_id",       m.get("request_id"));

        t.set("date_start",   toDate(epoch));       // DATE  (partition field)
        t.set("request_time", toDateTime(epoch));   // DATETIME “yyyy-MM-dd tttttt"

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
        t.set("custom_field",     m.get("custom_field_*"));

        return t;    
    }
}
