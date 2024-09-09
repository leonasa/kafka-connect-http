package com.github.castorm.kafka.connect.http.request.template;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 - 2024 Cástor Rodríguez
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

import com.github.castorm.kafka.connect.http.model.HttpRequest;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.request.spi.HttpRequestFactory;
import com.github.castorm.kafka.connect.http.request.template.spi.Template;
import com.github.castorm.kafka.connect.http.request.template.spi.TemplateFactory;
import lombok.Getter;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Map;

import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownHeaders;
import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownQueryParams;

@Getter
public class PaginatedTemplateHttpRequestFactory implements HttpRequestFactory {

    private String method;
    private Template urlTpl;
    private Template headersTpl;
    private Template queryParamsTpl;
    private Template bodyTpl;

    private int pageNumber = 1;
    private final int limit;
    private final String pageNumberParam;
    private final String limitParam;

    public PaginatedTemplateHttpRequestFactory(Map<String, ?> configs) {
        PaginatedTemplateHttpRequestFactoryConfig config = new PaginatedTemplateHttpRequestFactoryConfig(configs);

        TemplateFactory templateFactory = config.getTemplateFactory();
        this.method = config.getMethod();
        this.urlTpl = templateFactory.create(config.getUrl());
        this.headersTpl = templateFactory.create(config.getHeaders());
        this.queryParamsTpl = templateFactory.create(config.getQueryParams());
        this.bodyTpl = templateFactory.create(config.getBody());

        this.pageNumberParam = config.getPageNumber();
        this.limitParam = config.getLimitParam();
        this.limit = config.getLimit();
    }
    
    public void configure(Map<String, ?> configs) {
        PaginatedTemplateHttpRequestFactoryConfig config = new PaginatedTemplateHttpRequestFactoryConfig(configs);
        TemplateFactory templateFactory = config.getTemplateFactory();
        
        method = config.getMethod();
        urlTpl = templateFactory.create(config.getUrl());
        headersTpl = templateFactory.create(config.getHeaders());
        queryParamsTpl = templateFactory.create(config.getQueryParams());
        bodyTpl = templateFactory.create(config.getBody());
    }

    public HttpRequest createRequest(Offset offset) {
        // Apply the offset to the templates
        String url = urlTpl.apply(offset);
        String headers = headersTpl.apply(offset);
        String queryParams = queryParamsTpl.apply(offset);
        String body = bodyTpl.apply(offset);

        // Add pagination parameters to the URL
        String updatedQueryParams = String.format("%s&%s=%d&%s=%d",
                queryParams, pageNumberParam, pageNumber, limitParam, limit);

        String uriWithPagination = null;
        try {
            uriWithPagination = URI.create(url + "?" + updatedQueryParams).toURL().toString();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        return HttpRequest.builder()
                .method(HttpRequest.HttpMethod.valueOf(method))
                .url(uriWithPagination)
                .headers(breakDownHeaders(headers))
                .body(body.getBytes())
                .build();
    }
    
    public void incrementPage() {
        pageNumber++;
    }

    public void resetPage() {
        pageNumber = 1;
    }
}
