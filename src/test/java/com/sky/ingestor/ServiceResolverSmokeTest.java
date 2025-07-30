package com.sky.ingestor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

/*
 *   mvn -Dtest=ServiceResolverSmokeTest test
 */

public class ServiceResolverSmokeTest {

    private static String tok(String[] a, int idx){
        return (a != null && idx >= 0 && idx < a.length) ? a[idx] : "";
    }

    private static Map<String,String> parseMinimal(String model, String fullLine){
        Map<String,String> m = new HashMap<>();
        m.put("_model", model.toLowerCase());
        if (fullLine == null) return m;

        String[] t = fullLine.split("\\s+");

        switch (model.toLowerCase()) {
            case "skycdn":
                m.put("requestedHost", tok(t, 4));
                m.put("csUri",         tok(t, 5));
                break;

            case "akamai":
                m.put("request_host", tok(t, 6));
                m.put("request_path", tok(t, 7));
                break;

            case "cloudfront":
                m.put("xHostHeader", tok(t, 15));
                m.put("csUriStem",   tok(t, 7));
                break;

            case "raiway":
                m.put("xHostHeader", tok(t, 15));
                m.put("csUriStem",   tok(t, 7));
                break;

            default:
        }
        return m;
    }

    @Test
    void skycdn_nowtv_linear_example() {
        String model = "skycdn";
        String line  =
            "2025-07-08T10:02:08Z 2a0e:424:5c87:0:b915:c6d8:9f3c:47aa sn-ec0106-mid41 443 - " +
            "https://lin1-it-s8-prd-sn-mume1.cdn03.skycdp.com/v2/bmff/cenc/t/IT5459_HD_SI_SKYIT_5459_0_5179407976417923163/track-iframe-periodid-912482284-repid-iframe0-tc-0-frag-912483786.mp4 " +
            "GET 200 0 52155 000 0 FIN FIN TCP_MEM_HIT NONE - - 2a0e:404:103::6 Mozilla/5.0%20%28Linux%3B%20x86_64%20GNU/Linux%29%20AppleWebKit/601.1%20%28KHTML%2C%20like%20Gecko%29%20Version/8.0%20Safari/601.1%20WPE%20FOG/3.0.0 - 0 - - -1 - - - http/1.1 ";

        Map<String,String> row = parseMinimal(model, line);
        String service = ServiceResolver.resolveService(row);

        System.out.println("MODEL=" + model + " HOST=" + row.get("requestedHost") + " PATH=" + row.get("csUri") + " → SERVICE=" + service);
        assertEquals("soip_linear", service);
    }

    @Test
    void akamai_nowtv_linear_smoke() {
        String model = "akamai";
        String line  =
            "6 1650382 1bc80850 1752128895.220 185.25.206.9 200 g003-lin-it-cmaf-prd-ak.pcdn07.cssott02.com " +
            "nowitlin1/Content/CMAF_CTR_S1/Live/channel(historychannel)/master_2hr-aac.mpd application/dash+xml UA 3847 - 3 1 - ...";

        Map<String,String> row = parseMinimal(model, line);
        String service = ServiceResolver.resolveService(row);

        System.out.println("MODEL=" + model + " HOST=" + row.get("request_host") + " PATH=" + row.get("request_path") + " → SERVICE=" + service);
        assertEquals("nowtv_linear", service);
    }

    @Test
    void cloudfront_nomatch_smoke() {
        String model = "skycdn";
        String line =
            "2025-07-27T08:44:59Z 101.56.215.179 sn-ec0106-mica1 80 vod-stb-sn.prd-origin.skycdn.it http://sn-ec0106-mica1.it-http-vod-stb-sn.ds.c02.skycdp.com/live/HD/sky/AUXILIARY_ASSET_INSTANCE/sky-IDEC0000000000000680-202105191440400000.nff?u=41481991&c=i&s=n&p=559079261&a=2&t=s GET 206 0 188 200 188 FIN FIN TCP_MISS DIRECT 101.56.215.179 vod-stb-sn.prd-origin.skycdn.it 101.62.241.69 GW-Device/1.0.0%20(Sky,%20ESi160,%20) urn:sky:vod:skyqgw:unknown 0 0 text/plain 0 - -,-,-,- 622 HTTP/1.1";

        Map<String,String> row = parseMinimal(model, line);
        String service = ServiceResolver.resolveService(row);

        System.out.println("MODEL=" + model + " HOST=" + row.get("xHostHeader") + " PATH=" + row.get("csUriStem") + " → SERVICE=" + service);
        assertEquals("vod_stb", service);
    }
}
