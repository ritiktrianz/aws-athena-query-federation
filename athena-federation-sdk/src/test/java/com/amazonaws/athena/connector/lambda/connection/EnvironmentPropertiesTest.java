/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connector.lambda.connection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.AuthenticationConfiguration;
import software.amazon.awssdk.services.glue.model.Connection;
import software.amazon.awssdk.services.glue.model.ConnectionPropertyKey;
import software.amazon.awssdk.services.glue.model.GetConnectionRequest;
import software.amazon.awssdk.services.glue.model.GetConnectionResponse;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.KMS_KEY_ID;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SPILL_KMS_KEY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class EnvironmentPropertiesTest {

    // Given
    private String glueConnName = "my-glue-conn";
    private String secretArn = "arn:aws:secretsmanager:us-east-1:1234567890:secret:my-secret-abc123";
    private String expectedSecretName = "my-secret";

    @Before
    public void setUp()
            throws Exception {
        // Mock env
        Map<String, String> environmentVariables = new HashMap<>();
        environmentVariables.put(DEFAULT_GLUE_CONNECTION, glueConnName);
        environmentVariables.put("GLUE_ENDPOINT", "https://glue.mock.aws");
        environmentVariables.put("OVERRIDE_VAR", "lambda-value"); // Simulate Lambda-provided env var

        setEnvironmentVariables(environmentVariables);
    }

    @Test
    public void testCreateEnvironment() {

        // Create a partial mock so we can stub getGlueConnection
        EnvironmentProperties spyProps = spy(new EnvironmentProperties());

        // Mock Glue connection
        AuthenticationConfiguration authConfig = AuthenticationConfiguration.builder()
                .secretArn(secretArn)
                .build();

        Map<ConnectionPropertyKey, String> connectionProps = new HashMap<>();
        connectionProps.put(ConnectionPropertyKey.DATABASE, "example_value");

        Map<String, String> athenaProps = new HashMap<>();
        athenaProps.put(SPILL_KMS_KEY_ID, "kms-123");

        Connection glueConnection = Connection.builder()
                .name(glueConnName)
                .connectionProperties(connectionProps)
                .authenticationConfiguration(authConfig)
                .athenaProperties(athenaProps)
                .build();

        doReturn(glueConnection).when(spyProps).getGlueConnection(glueConnName);

        Map<String, String> result = spyProps.createEnvironment();

        assertEquals(glueConnName, result.get(DEFAULT_GLUE_CONNECTION));
        assertEquals("example_value", result.get(DATABASE));
        assertEquals(expectedSecretName, result.get(SECRET_NAME));
        assertEquals("kms-123", result.get(KMS_KEY_ID));
        assertEquals("lambda-value", result.get("OVERRIDE_VAR"));
    }

    @Test
    public void testGetGlueConnection() {
        // Create mocks
        GlueClient mockGlueClient = mock(GlueClient.class);
        GlueClientBuilder mockBuilder = mock(GlueClientBuilder.class);
        ApacheHttpClient.Builder mockHttpBuilder = mock(ApacheHttpClient.Builder.class);

        Connection mockConnection = Connection.builder().name(glueConnName).build();
        GetConnectionResponse mockResponse = GetConnectionResponse.builder().connection(mockConnection).build();

        try (
                MockedStatic<GlueClient> glueStatic = mockStatic(GlueClient.class);
                MockedStatic<ApacheHttpClient> httpStatic = mockStatic(ApacheHttpClient.class)
        ) {
            glueStatic.when(GlueClient::builder).thenReturn(mockBuilder);
            httpStatic.when(ApacheHttpClient::builder).thenReturn(mockHttpBuilder);

            when(mockHttpBuilder.connectionTimeout(any(Duration.class))).thenReturn(mockHttpBuilder);
            when(mockBuilder.httpClientBuilder(mockHttpBuilder)).thenReturn(mockBuilder);
            when(mockBuilder.endpointOverride(any())).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockGlueClient);
            when(mockGlueClient.getConnection(any(GetConnectionRequest.class))).thenReturn(mockResponse);

            // Test class
            EnvironmentProperties props = new EnvironmentProperties();
            Connection result = props.getGlueConnection(glueConnName);

            assertEquals(glueConnName, result.name());
        }
    }

    // Utility to override environment variables in tests
    private static void setEnvironmentVariables(Map<String, String> newEnvironmentVariables) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            java.lang.reflect.Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.clear();
            writableEnv.putAll(newEnvironmentVariables);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set environment variables", e);
        }
    }
}