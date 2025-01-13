package org.sunbird.workflow;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.util.Timeout;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class WorkflowApplication {

	public static void main(String[] args) {
		SpringApplication.run(WorkflowApplication.class, args);
	}

	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
	}

	@Bean
	public RestTemplate restTemplate() throws Exception {
		return new RestTemplate(getClientHttpRequestFactory());
	}

	private ClientHttpRequestFactory getClientHttpRequestFactory() {
		int timeout = 45000;
		RequestConfig config = RequestConfig.custom().
				setConnectTimeout(Timeout.ofMilliseconds(timeout)).
				setConnectionRequestTimeout(Timeout.ofMilliseconds(timeout)).
				setResponseTimeout(Timeout.ofMilliseconds(timeout)).
				build();

		CloseableHttpClient client = HttpClientBuilder.create()
				.setDefaultRequestConfig(config).build();
		HttpComponentsClientHttpRequestFactory cRequestFactory = new HttpComponentsClientHttpRequestFactory(client);
		cRequestFactory.setConnectTimeout(timeout);
		return cRequestFactory;
	}

}
