package com.sky.ingestor.utils;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Map;

public class RowMapperCloudfront implements RowMapper {
    @Override public TableRow toTableRow(Map<String,String> m){
        TableRow t = new TableRow();
        t.set("date",         m.get("date"));
        t.set("time",         m.get("time"));
        t.set("x_edge_location", m.get("xEdgeLocation"));
        t.set("sc_bytes",     m.get("scBytes"));
        t.set("cs_ip",        m.get("csIp"));
        // aggiungere gli altri campi

        t.set("_model",   m.get("_model"));
        t.set("_service", m.get("_service"));
        return t;
    }
}
