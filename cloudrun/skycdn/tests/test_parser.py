from parser import destination_for, map_to_bq_row, parse_fields, resolve_service


def test_skycdn_soip_linear_example():
    line = (
        "2025-07-08T10:02:08Z 2a0e:424:5c87:0:b915:c6d8:9f3c:47aa sn-ec0106-mid41 443 - "
        "https://lin1-it-s8-prd-sn-mume1.cdn03.skycdp.com/v2/bmff/cenc/t/IT5459_HD_SI_SKYIT_5459_0_5179407976417923163/track-iframe-periodid-912482284-repid-iframe0-tc-0-frag-912483786.mp4 "
        "GET 200 0 52155 000 0 FIN FIN TCP_MEM_HIT NONE - - 2a0e:404:103::6 "
        "Mozilla/5.0%20%28Linux%3B%20x86_64%20GNU/Linux%29%20AppleWebKit/601.1%20%28KHTML%2C%20like%20Gecko%29%20Version/8.0%20Safari/601.1%20WPE%20FOG/3.0.0 "
        "- 0 - - -1 - - - http/1.1"
    )
    parsed = parse_fields(line)
    assert resolve_service(parsed) == "soip_linear"
    assert destination_for("soip_linear") == ("it_skycdn_soip_logs", "soip_linear")

    row = map_to_bq_row(parsed, "CDN_ITA/skycdn/example.gz", "2026-09-28 12:00:00.000")
    assert row["http_status_code"] == 200
    assert row["request_host"] == "-"
    assert row["request_path"].endswith(".mp4")
    assert row["file_name"] == "CDN_ITA/skycdn/example.gz"
