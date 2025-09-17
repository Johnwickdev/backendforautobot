package com.trader.backend.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
public class CorsConfig {

  @Value("${cors.allowedOrigins:https://frontendfortheautobot.vercel.app,http://localhost:4200}")
  private String allowedOrigins;

  @Bean
  public CorsFilter corsFilter() {
    CorsConfiguration config = new CorsConfiguration();
    // If Authorization/cookies are needed, keep true and keep explicit origins
    config.setAllowCredentials(true);

    List<String> origins = Arrays.stream(allowedOrigins.split(","))
        .map(String::trim)
        .collect(Collectors.toList());
    config.setAllowedOrigins(origins);

    config.setAllowedHeaders(List.of(
        "Authorization", "Content-Type", "Accept", "Origin", "Cache-Control", "X-Requested-With"
    ));
    config.setAllowedMethods(List.of("GET","POST","PUT","DELETE","OPTIONS"));
    config.setExposedHeaders(List.of("Location"));

    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", config);
    return new CorsFilter(source);
  }
}
