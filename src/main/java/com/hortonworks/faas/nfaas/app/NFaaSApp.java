package com.hortonworks.faas.nfaas.app;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.netflix.zuul.EnableZuulProxy;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.oauth2.common.exceptions.OAuth2Exception;
import org.springframework.security.oauth2.config.annotation.configurers.ClientDetailsServiceConfigurer;
import org.springframework.security.oauth2.config.annotation.web.configuration.AuthorizationServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableAuthorizationServer;
import org.springframework.security.oauth2.config.annotation.web.configurers.AuthorizationServerEndpointsConfigurer;
import org.springframework.security.oauth2.config.annotation.web.configurers.AuthorizationServerSecurityConfigurer;
import org.springframework.security.oauth2.provider.error.DefaultWebResponseExceptionTranslator;
import org.springframework.security.oauth2.provider.error.WebResponseExceptionTranslator;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;


//@EnableGlobalMethodSecurity

//@EnableResourceServer
//@EnableAuthorizationServer
@SpringBootApplication
@EnableZuulProxy
@EnableJpaRepositories("com.hortonworks.faas.nfaas.*")
@EntityScan("com.hortonworks.faas.nfaas.*")
@ComponentScan({"com.hortonworks.faas.nfaas", "org.apache.nifi.web.api.dto", "com.hortonworks.faas.nfaas.core", "com.hortonworks.faas.nfaas.orm"})
@PropertySource(ignoreResourceNotFound = false, value = "classpath:application.properties")
@PropertySource(ignoreResourceNotFound = true, value = "file:/etc/hdfm/hdfm.properties")
public class NFaaSApp {

    private static final Logger logger = LoggerFactory.getLogger(NFaaSApp.class);

    public static void main(String[] args) {

        SpringApplication.run(NFaaSApp.class, args);
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) throws Exception {
        return builder.build();
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {

            logger.info("Let's inspect the beans provided by Spring Boot:");

            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                logger.warn(String.format("beanName %s", beanName));
            }

        };
    }

    @Bean
    public Jackson2ObjectMapperBuilder jackson2ObjectMapperBuilder() {

        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        AnnotationIntrospector annotationIntrospector = AnnotationIntrospector.pair(
                new JaxbAnnotationIntrospector(mapper.getTypeFactory()), new JacksonAnnotationIntrospector());

        builder.annotationIntrospector(annotationIntrospector);
        // This is to make jackson respect @XmlElementWrapper annotation and use wrapper name as the property name
        builder.defaultUseWrapper(true);
        return builder;
    }

//	@Bean
//	public Jackson2ObjectMapperBuilder jacksonBuilder() {
//	    Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
//	    builder.indentOutput(true).
//	    		dateFormat(new SimpleDateFormat("HH:mm:ss z"));
//	    return builder;
//	}


    @Configuration
    @EnableAuthorizationServer
    protected static class OAuth2Config extends AuthorizationServerConfigurerAdapter {

        @Autowired
        private AuthenticationManager authenticationManager;

        @Override
        public void configure(AuthorizationServerEndpointsConfigurer endpoints) throws Exception {
            endpoints.authenticationManager(authenticationManager)
                    .exceptionTranslator(loggingExceptionTranslator());
        }

        @Override
        public void configure(
                AuthorizationServerSecurityConfigurer oauthServer)
                throws Exception {
            oauthServer
                    .tokenKeyAccess("permitAll()")
                    .checkTokenAccess("isAuthenticated()");
        }

        @Override
        public void configure(ClientDetailsServiceConfigurer clients) throws Exception {

            clients.inMemory().withClient("acme").secret("acmesecret")
                    .authorizedGrantTypes("authorization_code", "refresh_token", "password").scopes("openid");
        }


    }

    @Bean
    public static WebResponseExceptionTranslator loggingExceptionTranslator() {
        return new DefaultWebResponseExceptionTranslator() {
            @Override
            public ResponseEntity<OAuth2Exception> translate(Exception e) throws Exception {
                // This is the line that prints the stack trace to the log. You can customise this to format the trace etc if you like
                e.printStackTrace();

                // Carry on handling the exception
                ResponseEntity<OAuth2Exception> responseEntity = super.translate(e);
                HttpHeaders headers = new HttpHeaders();
                headers.setAll(responseEntity.getHeaders().toSingleValueMap());
                OAuth2Exception excBody = responseEntity.getBody();
                return new ResponseEntity<>(excBody, headers, responseEntity.getStatusCode());
            }
        };
    }
}
