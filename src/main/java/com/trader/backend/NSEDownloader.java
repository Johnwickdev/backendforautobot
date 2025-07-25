package com.trader.backend;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.util.zip.GZIPInputStream;

public class NSEDownloader {

    public static void downloadAndExtractNSEFile() {
        try {
            String downloadUrl = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz";
            Path targetFolder = Paths.get("src/main/resources/data");
            Files.createDirectories(targetFolder);

            Path gzPath = targetFolder.resolve("NSE.json.gz");
            Path jsonPath = targetFolder.resolve("NSE.json");

            // Download
            try (InputStream in = new URL(downloadUrl).openStream()) {
                Files.copy(in, gzPath, StandardCopyOption.REPLACE_EXISTING);
                System.out.println("✅ NSE.json.gz downloaded.");
            }

            // Extract
            try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(gzPath.toFile()));
                 FileOutputStream out = new FileOutputStream(jsonPath.toFile())) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = gis.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
                System.out.println("✅ NSE.json extracted.");
            }

        } catch (Exception e) {
            System.err.println("❌ Failed to download/extract NSE file: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        downloadAndExtractNSEFile();
    }
}
