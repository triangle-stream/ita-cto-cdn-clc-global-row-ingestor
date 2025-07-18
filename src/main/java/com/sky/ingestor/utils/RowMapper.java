package com.sky.ingestor.utils;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Map;

public interface RowMapper {
    TableRow toTableRow(Map<String,String> parsed);
}