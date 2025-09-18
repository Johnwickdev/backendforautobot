package com.trader.backend.config;

import com.upstox.ApiClient;
import com.upstox.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

@org.springframework.context.annotation.Configuration
public class UpstoxClientConfig {

  @Value("${upstox.baseUrl:https://api.upstox.com/v3}")
  private String baseUrl;

  @Bean
  public ApiClient upstoxApiClient() {
    // This call touches com.squareup.okhttp.*; it will fail if OkHttp v2 is missing.
    ApiClient client = Configuration.getDefaultApiClient();
    client.setBasePath(baseUrl);
    // Do not set token here; your UpstoxAuthService should do per-request auth header.
    return client;
  }
}
