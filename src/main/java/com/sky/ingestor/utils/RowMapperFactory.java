package com.sky.ingestor.utils;

import java.util.Map;

public class RowMapperFactory {

    private static final Map<String,RowMapper> BY_MODEL =
        Map.of(
           "akamai",      new RowMapperAkamai(),
           "cloudfront",  new RowMapperCloudfront(),
           "skycdn",      new RowMapperSkycdn(),
           "raiway", new RowMapperRaiway()
        );

    public static RowMapper get(String model){
        return BY_MODEL.getOrDefault(model, row -> {
            throw new IllegalStateException("No RowMapper for model "+model);
        });
    }
}
