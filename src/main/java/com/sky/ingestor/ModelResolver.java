package com.sky.ingestor;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.*;

public class ModelResolver {
    private static final String[] modelNames = {"akamai", "cloudfront", "raiway"};

    public static String detectModel(String filename) {
        String name = filename.toLowerCase();
        if (name.contains("akamai")) return "akamai";
        if (name.contains("cloudfront")) return "cloudfront";
        if (name.contains("raiway")) return "raiway";
        return null;
    }

    public static Map<String, Map<String, Integer>> loadAllModels() throws Exception {
        Map<String, Map<String, Integer>> models = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        for (String modelName : modelNames) {
            InputStream is = ModelResolver.class.getClassLoader()
                .getResourceAsStream("models/" + modelName + "_model.json");
            if (is != null) {
                Map<String, Integer> model = mapper.readValue(is, HashMap.class);
                models.put(modelName, model);
            }
        }
        return models;
    }
}
