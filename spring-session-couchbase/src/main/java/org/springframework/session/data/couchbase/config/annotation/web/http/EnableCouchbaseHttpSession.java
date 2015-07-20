/*
 * Copyright 2002-2015 the original author or authors.
 *
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
 */
package org.springframework.session.data.couchbase.config.annotation.web.http;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Add this annotation to an {@code @Configuration} class to expose the
 * SessionRepositoryFilter as a bean named "springSessionRepositoryFilter" and
 * backed by Couchbase. In order to leverage the annotation, a single {@link com.couchbase.client.java.AsyncBucket}
 * must be provided. For example:
 *
 * <pre>
 * {@literal @Configuration}
 * {@literal @EnableCouchbaseHttpSession}
 * public class CouchbaseHttpSessionConfig {
 *
 *     {@literal @Bean}
 *     public AsyncBucket connectionFactory() throws Exception {
 *         CouchbaseCluster cc =  CouchbaseCluster.create();
 *         return cc.openBucket("default").async();
 *     }
 *
 * }
 * </pre>
 *
 * More advanced configurations can extend {@link CouchbaseHttpSessionConfiguration} instead.
 *
 * @author Laurent Doguin
 * @since 1.0
 */
@Retention(value=java.lang.annotation.RetentionPolicy.RUNTIME)
@Target(value={java.lang.annotation.ElementType.TYPE})
@Documented
@Import(CouchbaseHttpSessionConfiguration.class)
@Configuration
public @interface EnableCouchbaseHttpSession {
	int maxInactiveIntervalInSeconds() default 1800;
}