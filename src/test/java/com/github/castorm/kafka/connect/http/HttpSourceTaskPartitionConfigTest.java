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
import com.github.castorm.kafka.connect.http.model.HttpRequest;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.model.Partition;
import com.github.castorm.kafka.connect.http.record.OffsetRecordFilterFactory;
import com.github.castorm.kafka.connect.http.record.OrderDirectionSourceRecordSorter;
import com.github.castorm.kafka.connect.http.record.PassthroughRecordFilterFactory;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordSorter;
import com.github.castorm.kafka.connect.http.request.spi.HttpRequestFactory;
import com.github.castorm.kafka.connect.http.request.template.TemplateHttpRequestFactory;
import com.github.castorm.kafka.connect.http.response.PolicyHttpResponseParser;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import com.github.castorm.kafka.connect.timer.AdaptableIntervalTimer;
import com.github.castorm.kafka.connect.timer.FixedIntervalTimer;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.castorm.kafka.connect.http.HttpSourceTaskPartitionConfigTest.Fixture.config;
import static com.github.castorm.kafka.connect.http.HttpSourceTaskPartitionConfigTest.Fixture.configWithout;
import static com.github.castorm.kafka.connect.http.HttpSourceTaskPartitionConfigTest.Fixture.defaultMap;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

class HttpSourceTaskPartitionConfigTest {

    @Test
    void whenNoTimer_thenDefault() {
        assertThat(configWithout("http.timer").getTimer()).isInstanceOf(AdaptableIntervalTimer.class);
    }

    @Test
    void whenTimer_thenInitialized() {
        assertThat(config("http.timer", "com.github.castorm.kafka.connect.timer.FixedIntervalTimer").getTimer()).isInstanceOf(FixedIntervalTimer.class);
    }

    @Test
    void whenNoClient_thenDefault() {
        assertThat(configWithout("http.client").getClient()).isInstanceOf(OkHttpClient.class);
    }

    @Test
    void whenClient_thenInitialized() {
        assertThat(config("http.client", TestHttpClient.class.getName()).getClient()).isInstanceOf(TestHttpClient.class);
    }

    @Test
    void whenNoRequestFactory_thenDefault() {
        assertThat(configWithout("http.request.factory").getRequestFactory()).isInstanceOf(TemplateHttpRequestFactory.class);
    }

    @Test
    void whenRequestFactory_thenInitialized() {
        assertThat(config("http.request.factory", TestRequestFactory.class.getName()).getRequestFactory()).isInstanceOf(TestRequestFactory.class);
    }

    @Test
    void whenNoResponseParser_thenDefault() {
        assertThat(configWithout("http.response.parser").getResponseParser()).isInstanceOf(PolicyHttpResponseParser.class);
    }

    @Test
    void whenResponseParser_thenInitialized() {
        assertThat(config("http.response.parser", TestResponseParser.class.getName()).getResponseParser()).isInstanceOf(TestResponseParser.class);
    }

    @Test
    void whenNoRecordSorter_thenDefault() {
        assertThat(configWithout("http.record.sorter").getRecordSorter()).isInstanceOf(OrderDirectionSourceRecordSorter.class);
    }

    @Test
    void whenRecordSorter_thenInitialized() {
        assertThat(config("http.record.sorter", TestRecordSorter.class.getName()).getRecordSorter()).isInstanceOf(TestRecordSorter.class);
    }

    @Test
    void whenNoResponseFilterFactory_thenDefault() {
        assertThat(configWithout("http.record.filter.factory").getRecordFilterFactory()).isInstanceOf(OffsetRecordFilterFactory.class);
    }

    @Test
    void whenResponseFilterFactory_thenInitialized() {
        assertThat(config("http.record.filter.factory", "com.github.castorm.kafka.connect.http.record.PassthroughRecordFilterFactory").getRecordFilterFactory()).isInstanceOf(PassthroughRecordFilterFactory.class);
    }

    public static class TestHttpClient implements HttpClient {
        public HttpResponse execute(HttpRequest request) { return null; }
    }

    public static class TestRequestFactory implements HttpRequestFactory {
        public HttpRequest createRequest(Partition partition, Offset offset) { return null; }
    }

    public static class TestResponseParser implements HttpResponseParser {
        public List<SourceRecord> parse(HttpResponse response, Partition partition) { return null; }
    }

    public static class TestRecordSorter implements SourceRecordSorter {
        public List<SourceRecord> sort(List<SourceRecord> records) { return null; }
    }

    interface Fixture {
        Partition partition = Partition.of("test", emptyMap());

        static Map<String, String> defaultMap() {
            return new HashMap<String, String>() {{
                put("kafka.topic", "topic");
                put("http.request.url", "foo");
                put("http.response.json.record.offset1.value.pointer", "/baz");
            }};
        }

        static HttpSourceTaskPartitionConfig config(String key, String value) {
            Map<String, String> customMap = defaultMap();
            customMap.put(key, value);
            return new HttpSourceTaskPartitionConfig(partition, customMap);
        }

        static HttpSourceTaskPartitionConfig configWithout(String key) {
            Map<String, String> customMap = defaultMap();
            customMap.remove(key);
            return new HttpSourceTaskPartitionConfig(partition, customMap);
        }
    }
}
