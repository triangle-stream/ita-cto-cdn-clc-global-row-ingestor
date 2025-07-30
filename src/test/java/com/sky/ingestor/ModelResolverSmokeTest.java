package com.sky.ingestor;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

 /* 
 *   mvn -Dtest=ModelResolverSmokeTest test
 */

public class ModelResolverSmokeTest {

    @Test
    @DisplayName("detectModel: riconosce i modelli per filename tipici (case-insensitive)")
    void detectModel_basicCases() {
        // akamai
        assertEquals("akamai",
            ModelResolver.detectModel("gs://bucket/CDN_ITA/20250729-akAMai-nowtv-linear-001.gz"));

        // cloudfront
        assertEquals("cloudfront",
            ModelResolver.detectModel("gs://bucket/CDN_ITA/20250729-cloudfront-logs-abc.gz"));

        // cloudfront_legacy
        assertEquals("cloudfront_legacy",
            ModelResolver.detectModel("gs://bucket/CDN_ITA/20250729-awscdn-legacy-001.gz"));

        // raiway 
        assertEquals("raiway",
            ModelResolver.detectModel("gs://bucket/CDN_ITA/20250729-raiway-xyz.gz"));

        // skycdn 
        assertEquals("skycdn",
            ModelResolver.detectModel("gs://bucket/CDN_ITA/250708T101100_sn-ec0107-mica1.c02.skycdp.com_...gz"));

        // unknown → null
        assertNull(ModelResolver.detectModel("gs://bucket/CDN_ITA/20250729-unknownvendor-001.gz"));
    }

    @Test
    @DisplayName("loadAllModels")
    void loadAllModels_shouldLoadPresentOnes() throws Exception {
        Map<String, Map<String, Integer>> models = ModelResolver.loadAllModels();

        assertTrue(models.containsKey("akamai"), "Missing akamai_model.json on classpath");
        assertTrue(models.containsKey("cloudfront"), "Missing cloudfront_model.json on classpath");
        assertTrue(models.containsKey("cloudfront_legacy"), "Missing cloudfront_legacy_model.json on classpath");
        assertTrue(models.containsKey("skycdn"), "Missing skycdn_model.json on classpath");
        // assertTrue(models.containsKey("raiway")); // <-- scommenta quando aggiungi raiway_model.json

        assertFalse(models.get("akamai").isEmpty(), "akamai model is empty");
        assertFalse(models.get("cloudfront").isEmpty(), "cloudfront model is empty");
        assertFalse(models.get("cloudfront_legacy").isEmpty(), "cloudfront_legacy model is empty");
        assertFalse(models.get("skycdn").isEmpty(), "skycdn model is empty");
    }
}
