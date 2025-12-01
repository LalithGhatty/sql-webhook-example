package com.example.sql_webhook_example;

import org.springframework.boot.SpringApplication;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.example.sql_webhook_example.service.*;

@SpringBootApplication
public class SqlWebhookExampleApplication implements CommandLineRunner {

    private final WebhookSqlService webhookSqlService;

    public SqlWebhookExampleApplication(WebhookSqlService webhookSqlService) {
        this.webhookSqlService = webhookSqlService;
    }

    public static void main(String[] args) {
        SpringApplication.run(SqlWebhookExampleApplication.class, args);
    }

    @Override
    public void run(String... args) {
        webhookSqlService.runFlow();
    }
}
