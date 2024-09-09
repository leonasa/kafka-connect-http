package com.github.castorm.kafka.connect.http.request.template;

import lombok.Getter;

import java.util.Map;

import com.github.castorm.kafka.connect.http.request.template.freemarker.BackwardsCompatibleFreeMarkerTemplateFactory;
import com.github.castorm.kafka.connect.http.request.template.spi.TemplateFactory;
import org.apache.kafka.common.config.ConfigDef;

import org.apache.kafka.common.config.AbstractConfig;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;

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
@Getter
public class PaginatedTemplateHttpRequestFactoryConfig extends AbstractConfig  {
    public static final String PAGE_NUMBER_PARAM_CONFIG = "pagination.pageNumberParam";
    public static final String LIMIT_PARAM_CONFIG = "pagination.limitParam";
    public static final String LIMIT_CONFIG = "pagination.limit";

    private static final String URL = "http.request.url";
    private static final String METHOD = "http.request.method";
    private static final String HEADERS = "http.request.headers";
    private static final String QUERY_PARAMS = "http.request.params";
    private static final String BODY = "http.request.body";
    private static final String TEMPLATE_FACTORY = "http.request.template.factory";

    private final String pageNumber;

    private final Integer limit;

    private final String limitParam;

    private final String url;

    private final String method;

    private final String headers;

    private final String queryParams;

    private final String body;

    private final TemplateFactory templateFactory;

    public static ConfigDef config() {
        return new ConfigDef()
                .define(URL,  ConfigDef.Type.STRING, HIGH, "HTTP URL Template")
                .define(METHOD,  ConfigDef.Type.STRING, "GET", HIGH, "HTTP Method Template")
                .define(HEADERS,  ConfigDef.Type.STRING, "", MEDIUM, "HTTP Headers Template")
                .define(QUERY_PARAMS,  ConfigDef.Type.STRING, "", MEDIUM, "HTTP Query Params Template")
                .define(BODY,  ConfigDef.Type.STRING, "", LOW, "HTTP Body Template")
                .define(TEMPLATE_FACTORY, CLASS, BackwardsCompatibleFreeMarkerTemplateFactory.class, LOW, "Template Factory Class")  .define(PAGE_NUMBER_PARAM_CONFIG, ConfigDef.Type.STRING, "pageNumber", ConfigDef.Importance.MEDIUM, "Query parameter for page number.")
                .define(LIMIT_PARAM_CONFIG, ConfigDef.Type.STRING, "limit", ConfigDef.Importance.MEDIUM, "Query parameter for page size.")
                .define(LIMIT_CONFIG, ConfigDef.Type.INT, 10, ConfigDef.Importance.MEDIUM, "Number of records per page.");
    }


    public PaginatedTemplateHttpRequestFactoryConfig(Map<String, ?> originals){
        super(config(), originals);
        pageNumber = getString(PAGE_NUMBER_PARAM_CONFIG);
        limit = getInt(LIMIT_CONFIG);
        limitParam = getString(LIMIT_PARAM_CONFIG);
        url = getString(URL);
        method = getString(METHOD);
        headers = getString(HEADERS);
        queryParams = getString(QUERY_PARAMS);
        body = getString(BODY);
        templateFactory = getConfiguredInstance(TEMPLATE_FACTORY, TemplateFactory.class);
    }
}
