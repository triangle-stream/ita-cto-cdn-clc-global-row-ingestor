package com.sky.ingestor.utils;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.google.api.services.bigquery.model.TableRow;

public class RowMapperSkycdn implements RowMapper {

    private static final DateTimeFormatter DT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                         .withZone(ZoneOffset.UTC);

    private static Integer toInt(String s){ try{ return Integer.valueOf(s); }catch(Exception e){return null;} }
    private static Double  toDbl(String s){ try{ return Double.valueOf(s); }catch(Exception e){return null;} }
    private static Float   toFlt(String s){ try{ return Float.valueOf(s); }catch(Exception e){return null;} }

    private static String toDate(String isoZ){
        return Instant.parse(isoZ).atZone(ZoneOffset.UTC).toLocalDate().toString();   // "yyyy-MM-dd"
    }
    private static String toDateTime(String isoZ){
        return DT.format(Instant.parse(isoZ));                                        // "yyyy-MM-dd HH:mm:ss.SSS"
    }
    private static String pathOf(String url){
        try { return URI.create(url).getRawPath(); }
        catch(Exception e){ return url; }
    }

    @Override
    public TableRow toTableRow(Map<String,String> m){

        String isoTs = m.get("datetime");   // 2024-12-20T11:34:31Z
        if (isoTs == null || isoTs.isEmpty()) return null;

        TableRow t = new TableRow();

        t.set("request_id", null); 

        t.set("date_start",   toDate(isoTs));      // DATE (stringa "yyyy-MM-dd")
        t.set("request_time", toDateTime(isoTs));  // DATETIME (stringa "yyyy-MM-dd HH:mm:ss.SSS")

        t.set("client_ip",        m.get("csIp"));
        t.set("http_status_code", toInt(m.get("pssc")));
        t.set("request_host",     m.get("requestedHost"));
        t.set("request_path",     pathOf(m.get("csUri")));
        t.set("response_content_type", m.get("psct"));
        t.set("user_agent",       m.get("csUserAgent"));

        Double totalBytes = toDbl(m.get("responseSize"));
        if (totalBytes == null) totalBytes = toDbl(m.get("csBytes"));
        t.set("total_bytes", totalBytes);

        t.set("error_code",        null);                         
        t.set("turnaround_time",   toFlt(m.get("requestTime"))); 
        t.set("transfer_time",     toFlt(m.get("ttfb")));        
        t.set("custom_field_unprocessed", null);

        t.set("cache_status",      null);  
        t.set("request_end_time",  null);  
        t.set("asnum",             null);  
        t.set("breadcrumbs",       null);  

        t.set("protocol_type",     m.get("csProtocol"));
        t.set("request_method",    m.get("httpMethod"));

        t.set("cookie",            null);  
        t.set("referer",           null);  
        t.set("edge_ip",           m.get("hii"));

        t.set("xmt",  m.get("xmt"));
        t.set("cmcd", m.get("cmcd"));
        t.set("crc",  m.get("crc"));
        t.set("phr",  m.get("phr"));

        return t;
    }
}
