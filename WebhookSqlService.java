package com.example.sql_webhook_example.service;



import com.example.sql_webhook_example.dto.FinalQueryRequest;
import com.example.sql_webhook_example.dto.GenerateWebhookRequest;
import com.example.sql_webhook_example.dto.GenerateWebhookResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class WebhookSqlService {

    private static final Logger log = LoggerFactory.getLogger(WebhookSqlService.class);

    private final RestTemplate restTemplate;

    
    private static final String CANDIDATE_NAME  = "Ghatty Venkata SaiKrishna Lalith";
    private static final String CANDIDATE_REGNO = "22BCE20329";
    private static final String CANDIDATE_EMAIL = "gvsklalith2005@gmail.com";

    private static final String GENERATE_WEBHOOK_URL =
            "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

    
    private static final String FINAL_SQL_QUERY =
            "WITH valid_payments AS ( " +
            "    SELECT p.emp_id, p.amount " +
            "    FROM PAYMENTS p " +
            "    WHERE EXTRACT(DAY FROM p.payment_time) <> 1 " +
            "), employee_totals AS ( " +
            "    SELECT e.emp_id, e.first_name, e.last_name, e.dob, e.department, " +
            "           SUM(v.amount) AS total_salary " +
            "    FROM EMPLOYEE e " +
            "    JOIN valid_payments v ON e.emp_id = v.emp_id " +
            "    GROUP BY e.emp_id, e.first_name, e.last_name, e.dob, e.department " +
            "), ranked AS ( " +
            "    SELECT d.department_name, " +
            "           et.total_salary AS salary, " +
            "           CONCAT(et.first_name, ' ', et.last_name) AS employee_name, " +
            "           FLOOR(DATEDIFF(CURDATE(), et.dob) / 365) AS age, " +
            "           ROW_NUMBER() OVER (PARTITION BY et.department ORDER BY et.total_salary DESC) AS rn " +
            "    FROM employee_totals et " +
            "    JOIN department d ON et.department = d.department_id " +
            ") " +
            "SELECT department_name, salary, employee_name, age " +
            "FROM ranked " +
            "WHERE rn = 1;";

    public WebhookSqlService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public void runFlow() {
       
        GenerateWebhookResponse response = generateWebhook();

        if (response == null || response.getWebhook() == null || response.getAccessToken() == null) {
            log.error("Failed to get webhook or accessToken from server");
            return;
        }

        String webhookUrl = response.getWebhook();
        String accessToken = response.getAccessToken();

        log.info("Received webhook URL: {}", webhookUrl);

       
        submitFinalQuery(webhookUrl, accessToken, FINAL_SQL_QUERY);
    }

    private GenerateWebhookResponse generateWebhook() {
        try {
            GenerateWebhookRequest requestBody =
                    new GenerateWebhookRequest(CANDIDATE_NAME, CANDIDATE_REGNO, CANDIDATE_EMAIL);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<GenerateWebhookRequest> entity = new HttpEntity<>(requestBody, headers);

            ResponseEntity<GenerateWebhookResponse> responseEntity =
                    restTemplate.exchange(
                            GENERATE_WEBHOOK_URL,
                            HttpMethod.POST,
                            entity,
                            GenerateWebhookResponse.class
                    );

            if (responseEntity.getStatusCode().is2xxSuccessful()) {
                return responseEntity.getBody();
            } else {
                log.error("generateWebhook returned status: {}", responseEntity.getStatusCode());
                return null;
            }

        } catch (Exception e) {
            log.error("Error while calling generateWebhook", e);
            return null;
        }
    }

    private void submitFinalQuery(String webhookUrl, String accessToken, String finalQuery) {
        try {
            FinalQueryRequest body = new FinalQueryRequest(finalQuery);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

           
            headers.set("Authorization", accessToken);

            HttpEntity<FinalQueryRequest> entity = new HttpEntity<>(body, headers);

            ResponseEntity<String> responseEntity =
                    restTemplate.exchange(
                            webhookUrl,
                            HttpMethod.POST,
                            entity,
                            String.class
                    );

            if (responseEntity.getStatusCode().is2xxSuccessful()) {
                log.info("Successfully submitted final query. Response: {}", responseEntity.getBody());
            } else {
                log.error("Failed to submit final query. Status: {}, Body: {}",
                        responseEntity.getStatusCode(), responseEntity.getBody());
            }

        } catch (Exception e) {
            log.error("Error while submitting final query to webhook", e);
        }
    }
}
