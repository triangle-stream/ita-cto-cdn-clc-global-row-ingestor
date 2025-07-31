package com.sky.ingestor;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;


    public class ModelResolver {
        private static final String[] modelNames = {"akamai", "cloudfront", "cloudfront_legacy","raiway", "skycdn"};

        public static String detectModel(String filename) {
        if (filename == null) return null;

        final String name = filename.toLowerCase();

        String base = filename;
        int slash = base.lastIndexOf('/');
        if (slash >= 0 && slash + 1 < base.length()) {
            base = base.substring(slash + 1);
        }

        if (!base.isEmpty() && base.charAt(0) == 'E') {
            return "cloudfront";
        }

        if (name.contains("aws") || name.contains("awscdn")) {
            return "cloudfront_legacy";
        }

        if (name.contains("akamai")) return "akamai";
        if (name.contains("raiway")) return "raiway";
        if (name.contains("sn-"))    return "skycdn";

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


