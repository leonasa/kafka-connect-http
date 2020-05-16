package com.github.castorm.kafka.connect.http;

/*-
 * #%L
 * kafka-connect-http
 * %%
 * Copyright (C) 2020 CastorM
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

import com.github.castorm.kafka.connect.http.client.okhttp.OkHttpClient;
import com.github.castorm.kafka.connect.http.client.spi.HttpClient;
import com.github.castorm.kafka.connect.http.record.PassthroughRecordFilterFactory;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import com.github.castorm.kafka.connect.http.request.spi.HttpRequestFactory;
import com.github.castorm.kafka.connect.http.request.template.TemplateHttpRequestFactory;
import com.github.castorm.kafka.connect.http.response.PolicyHttpResponseParser;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import com.github.castorm.kafka.connect.throttle.AdaptableIntervalThrottler;
import com.github.castorm.kafka.connect.throttle.spi.Throttler;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownMap;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

@Getter
class HttpSourceConnectorConfig extends AbstractConfig {

    private static final String THROTTLER = "http.throttler";
    private static final String CLIENT = "http.client";
    private static final String REQUEST_FACTORY = "http.request.factory";
    private static final String RESPONSE_PARSER = "http.response.parser";
    private static final String RECORD_FILTER_FACTORY = "http.record.filter.factory";
    private static final String OFFSET_INITIAL = "http.offset.initial";

    private final Throttler throttler;
    private final HttpRequestFactory requestFactory;
    private final HttpClient client;
    private final HttpResponseParser responseParser;
    private final SourceRecordFilterFactory recordFilterFactory;
    private final Map<String, String> initialOffset;

    HttpSourceConnectorConfig(Map<String, ?> originals) {
        super(config(), originals);
        throttler = getConfiguredInstance(THROTTLER, Throttler.class);
        requestFactory = getConfiguredInstance(REQUEST_FACTORY, HttpRequestFactory.class);
        client = getConfiguredInstance(CLIENT, HttpClient.class);
        responseParser = getConfiguredInstance(RESPONSE_PARSER, HttpResponseParser.class);
        recordFilterFactory = getConfiguredInstance(RECORD_FILTER_FACTORY, SourceRecordFilterFactory.class);
        initialOffset = breakDownMap(getString(OFFSET_INITIAL));
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(THROTTLER, CLASS, AdaptableIntervalThrottler.class, HIGH, "Poll Throttler Class")
                .define(CLIENT, CLASS, OkHttpClient.class, HIGH, "Request Client Class")
                .define(REQUEST_FACTORY, CLASS, TemplateHttpRequestFactory.class, HIGH, "Request Factory Class")
                .define(RESPONSE_PARSER, CLASS, PolicyHttpResponseParser.class, HIGH, "Response Parser Class")
                .define(RECORD_FILTER_FACTORY, CLASS, PassthroughRecordFilterFactory.class, LOW, "Record Filter Factory Class")
                .define(OFFSET_INITIAL, STRING, "", HIGH, "Starting offset");
    }
}
