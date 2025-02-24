/*-
 * #%L
 * Amazon Athena GCS Connector
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import software.amazon.awssdk.services.athena.model.AthenaException;

public class GcsThrottlingExceptionFilter implements ThrottlingInvoker.ExceptionFilter
{
    public static final ThrottlingInvoker.ExceptionFilter EXCEPTION_FILTER = new GcsThrottlingExceptionFilter();

    @Override
    public boolean isMatch(Exception ex)
    {
        return (ex instanceof AthenaException && ex.getMessage().contains("Rate exceeded"))
                || ex.getMessage().contains("Too Many Requests");
    }
}
