package com.sky.ingestor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ServiceResolverSmokeTest {

    static Map<String, Map<String,Integer>> MODELS;

    @BeforeAll
    static void loadModels() throws Exception {
        MODELS = ModelResolver.loadAllModels();
    }

    private Map<String,String> parseWithModel(String model, String line) {
        Map<String, Integer> mapping = MODELS.get(model);
        if (mapping == null) throw new IllegalArgumentException("Unknown model: " + model);

        String[] toks = TestHelpers.splitAccordingToModel(model, line);

        Map<String,String> row = new HashMap<>();
        mapping.forEach((k, idx) -> {
            String v = (idx != null && idx >= 0 && idx < toks.length) ? toks[idx] : "";
            row.put(k, TestHelpers.unquote(v));
        });

        row.put("_model", model);
        return row;
    }

    @Test
    void cloudfront_legacy_hip_live() {
        String model = "skycdn";
        String line =
            "2025-08-01T00:08:00Z 2a0e:431:72a:0:da6:351d:8115:94d9 sn-ec0103-mica1 443 - https://sn-ec0103-mica1.vod001-it-s8-prd.ds.c02.skycdp.com/v1/frag/bmff/enc/cenc/t/ipvod20/b3245488-455a-4528-b1c6-c88089a6f9af/1751779347/M/HD/manifest-eac3/track-audio-repid-DDit-tc-0-frag-3255.mp4 GET 200 0 47921 000 0 FIN FIN TCP_HIT NONE - - 2a0e:404:103::2:3 Mozilla/5.0%20%28X11%3B%20Linux%20armv7l%29%20AppleWebKit/605.1.15%20%28KHTML%2C%20like%20Gecko%29%20Version/16.0%20Safari/605.1.15%20WPE/1.0%20AAMP/6.12 - 0 - - -1 - - - http/1.1";

        Map<String,String> row = parseWithModel(model, line);
        String service = ServiceResolver.resolveService(row);
        System.out.println("MODEL=" + model + " HOST=" + row.get("requestedHost") + " PATH=" + row.get("csUri") + " → SERVICE=" + service + " | date_insert=");

        assertEquals("soip_ads", service);
    }

    @Test
    void skycdn_soip_linear_example() {
        String model = "skycdn";
        String line =
            "2025-07-08T10:02:08Z 2a0e:424:5c87:0:b915:c6d8:9f3c:47aa sn-ec0106-mid41 443 - " +
            "https://lin1-it-s8-prd-sn-mume1.cdn03.skycdp.com/v2/bmff/cenc/t/IT5459_HD_SI_SKYIT_5459_0_5179407976417923163/track-iframe-periodid-912482284-repid-iframe0-tc-0-frag-912483786.mp4 " +
            "GET 200 0 52155 000 0 FIN FIN TCP_MEM_HIT NONE - - 2a0e:404:103::6 Mozilla/5.0%20%28Linux%3B%20x86_64%20GNU/Linux%29%20AppleWebKit/601.1%20%28KHTML%2C%20like%20Gecko%29%20Version/8.0%20Safari/601.1%20WPE%20FOG/3.0.0 - 0 - - -1 - - - http/1.1";

        Map<String,String> row = parseWithModel(model, line);
        String service = ServiceResolver.resolveService(row);

        assertEquals("soip_linear", service);
    }

    @Test
    void akamai_nowtv_linear() {
        String model = "akamai";
        String line =
            "6 1650382 1bc80850 1752128895.220 185.25.206.9 200 g003-lin-it-cmaf-prd-ak.pcdn07.cssott02.com " +
            "nowitlin1/Content/CMAF_CTR_S1/Live/channel(historychannel)/master_2hr-aac.mpd application/dash+xml UA 3847 - 3 1 - ...";

        Map<String,String> row = parseWithModel(model, line);
        String service = ServiceResolver.resolveService(row);

        assertEquals("nowtv_linear", service);
    }
}
