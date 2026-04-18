package com.marioreis.ledgerquery.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class SwaggerConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Ledger Query Service API")
                        .version("1.0.0")
                        .description("CQRS read-side ledger query service. " +
                                "Provides account balances, transaction history, and monthly statements " +
                                "built from Kafka event projections. Independent of the command service.")
                        .contact(new Contact()
                                .name("Mario Soares Reis")
                                .url("https://github.com/mariosoaresreis/ledger")))
                .servers(List.of(
                        new Server()
                                .url("http://QUERY_EXTERNAL_IP")
                                .description("GKE Query DEV (us-east1) — replace with actual IP after deploy"),
                        new Server()
                                .url("http://localhost:8081")
                                .description("Local Development")));
    }
}

