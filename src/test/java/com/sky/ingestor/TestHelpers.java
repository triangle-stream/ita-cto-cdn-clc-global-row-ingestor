package com.sky.ingestor;

public final class TestHelpers {

    private TestHelpers() {}

    public static String[] splitAccordingToModel(String model, String line) {
        if (line == null) return new String[0];
        switch (model.toLowerCase()) {
            case "raiway":
            case "cloudfront_legacy":
                return line.split("\t", -1);   // TAB-separated
            default:
                return line.trim().split("\\s+"); // whitespace
        }
    }

    /** Rimuove le virgolette esterne "..." se presenti. */
    public static String unquote(String s){
        if (s == null) return null;
        s = s.trim();
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length()-1);
        }
        return s;
    }
}
