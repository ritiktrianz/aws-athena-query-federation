/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda;

import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.exceptions.FederationThrottleException;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.athena.connector.lambda.ThrottlingInvoker.THROTTLE_DECREASE_FACTOR;
import static com.amazonaws.athena.connector.lambda.ThrottlingInvoker.THROTTLE_INCREASE_MS;
import static com.amazonaws.athena.connector.lambda.ThrottlingInvoker.THROTTLE_INITIAL_DELAY_MS;
import static com.amazonaws.athena.connector.lambda.ThrottlingInvoker.THROTTLE_MAX_DELAY_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThrottlingInvokerTest
{

    @Test
    public void invokeNoThrottle()
            throws TimeoutException
    {
        ThrottlingInvoker invoker = ThrottlingInvoker.newBuilder()
                .withDecrease(0.5)
                .withIncrease(10)
                .withInitialDelayMs(10)
                .withMaxDelayMs(2_000)
                .withFilter((Exception ex) -> ex instanceof FederationThrottleException)
                .build();

        for (int i = 0; i < 100; i++) {
            //Make a call and validate that the state didn't change
            int result = invoker.invoke(() -> 1 + 1, 10_000);
            assertEquals(2, result);
            assertEquals(ThrottlingInvoker.State.FAST_START, invoker.getState());
            assertEquals(0, invoker.getDelay());
        }
    }

    @Test
    public void invokeWithThrottle()
            throws TimeoutException
    {
        ThrottlingInvoker invoker = ThrottlingInvoker.newBuilder()
                .withDecrease(0.8)
                .withIncrease(1)
                .withInitialDelayMs(10)
                .withMaxDelayMs(200)
                .withFilter((Exception ex) -> ex instanceof FederationThrottleException)
                .build();

        for (int i = 0; i < 5; i++) {
            //Make a call and validate that the state didn't change
            final AtomicLong count = new AtomicLong(0);
            final int val = i;
            long result = invoker.invoke(() -> {
                        if (count.incrementAndGet() < 4) {
                            throw new FederationThrottleException();
                        }
                        return val;
                    }
                    , 10_000);
            assertEquals(val, result);
            assertEquals(4, count.get());
            assertEquals(ThrottlingInvoker.State.AVOIDANCE, invoker.getState());
            assertTrue(invoker.getDelay() > 0);
        }

        assertEquals(199, invoker.getDelay());
    }

    @Test(expected = AthenaConnectorException.class)
    public void invokeWithThrottleTimeout()
            throws TimeoutException
    {
        ThrottlingInvoker invoker = ThrottlingInvoker.newBuilder()
                .withDecrease(0.5)
                .withIncrease(10)
                .withInitialDelayMs(10)
                .withMaxDelayMs(500)
                .withFilter((Exception ex) -> ex instanceof FederationThrottleException)
                .build();

        invoker.invoke(() -> {throw new AthenaConnectorException("Throttling error", ErrorDetails.builder().errorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION.toString()).build());}, 2_000);
    }

    @Test(expected = AthenaConnectorException.class)
    public void invokeWithThrottleNoSpill()
            throws TimeoutException
    {
        BlockSpiller spiller = mock(BlockSpiller.class);
        ThrottlingInvoker invoker = ThrottlingInvoker.newBuilder()
                .withDecrease(0.5)
                .withIncrease(10)
                .withInitialDelayMs(10)
                .withMaxDelayMs(500)
                .withFilter((Exception ex) -> ex instanceof RuntimeException)
                .withSpiller(spiller)
                .build();

        when(spiller.spilled()).thenReturn(false);
        invoker.invoke(() -> {throw new AthenaConnectorException("Throttling error", ErrorDetails.builder().errorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION.toString()).build());}, 2_000);
    }

    @Test
    public void testDefaultBuilderFromConfig()
    {
        java.util.Map<String, String> configOption = Map.of(THROTTLE_INITIAL_DELAY_MS, "100",
                THROTTLE_MAX_DELAY_MS, "10000",
                THROTTLE_DECREASE_FACTOR, "0.1",
                THROTTLE_INCREASE_MS, "500") ;
        ThrottlingInvoker.Builder builder = ThrottlingInvoker.newDefaultBuilder((Exception ex) -> ex instanceof FederationThrottleException, configOption);

        ThrottlingInvoker invoker = builder.build();

        assertTrue(invoker.getDecrease() == 0.1);
        assertEquals(invoker.getIncrease(), 500);
        assertEquals(invoker.getDefaultInitialDelayMs(), 100);
        assertEquals(invoker.getMaxDelayMs(), 10000);
    }
}
