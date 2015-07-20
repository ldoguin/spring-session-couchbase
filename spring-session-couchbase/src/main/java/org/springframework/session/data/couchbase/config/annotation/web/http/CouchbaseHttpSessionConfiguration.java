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

import com.couchbase.client.java.AsyncBucket;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.session.ExpiringSession;
import org.springframework.session.SessionRepository;
import org.springframework.session.data.couchbase.CouchbaseSessionRepository;
import org.springframework.session.web.http.HttpSessionStrategy;
import org.springframework.session.web.http.SessionRepositoryFilter;
import org.springframework.util.ClassUtils;

import javax.servlet.ServletContext;
import java.util.Map;

/**
 * Exposes the {@link org.springframework.session.web.http.SessionRepositoryFilter} as a bean named
 * "springSessionRepositoryFilter". In order to use this a single
 * {@link com.couchbase.client.java.AsyncBucket} must be exposed as a Bean.
 *
 * @author Laurent Doguin
 * @since 1.0
 *
 * @see EnableCouchbaseHttpSession
 */
@Configuration
@EnableScheduling
public class CouchbaseHttpSessionConfiguration implements ImportAware, BeanClassLoaderAware {

	private ClassLoader beanClassLoader;

	private Integer maxInactiveIntervalInSeconds = 1800;

	private HttpSessionStrategy httpSessionStrategy;

	@Autowired
	private ApplicationEventPublisher eventPublisher;

	@Bean
	public CouchbaseSessionRepository sessionRepository(AsyncBucket asyncBucket) {
		CouchbaseSessionRepository sessionRepository = new CouchbaseSessionRepository(asyncBucket);
		sessionRepository.setDefaultMaxInactiveInterval(maxInactiveIntervalInSeconds);
		return sessionRepository;
	}

	@Bean
	public <S extends ExpiringSession> SessionRepositoryFilter<? extends ExpiringSession> springSessionRepositoryFilter(SessionRepository<S> sessionRepository, ServletContext servletContext) {
		SessionRepositoryFilter<S> sessionRepositoryFilter = new SessionRepositoryFilter<S>(sessionRepository);
		sessionRepositoryFilter.setServletContext(servletContext);
		if(httpSessionStrategy != null) {
			sessionRepositoryFilter.setHttpSessionStrategy(httpSessionStrategy);
		}
		return sessionRepositoryFilter;
	}

	public void setMaxInactiveIntervalInSeconds(int maxInactiveIntervalInSeconds) {
		this.maxInactiveIntervalInSeconds = maxInactiveIntervalInSeconds;
	}

	public void setImportMetadata(AnnotationMetadata importMetadata) {

		Map<String, Object> enableAttrMap = importMetadata.getAnnotationAttributes(EnableCouchbaseHttpSession.class.getName());
		AnnotationAttributes enableAttrs = AnnotationAttributes.fromMap(enableAttrMap);
		if(enableAttrs == null) {
			// search parent classes
			Class<?> currentClass = ClassUtils.resolveClassName(importMetadata.getClassName(), beanClassLoader);
			for(Class<?> classToInspect = currentClass ;classToInspect != null; classToInspect = classToInspect.getSuperclass()) {
				EnableCouchbaseHttpSession enableWebSecurityAnnotation = AnnotationUtils.findAnnotation(classToInspect, EnableCouchbaseHttpSession.class);
				if(enableWebSecurityAnnotation == null) {
					continue;
				}
				enableAttrMap = AnnotationUtils
						.getAnnotationAttributes(enableWebSecurityAnnotation);
				enableAttrs = AnnotationAttributes.fromMap(enableAttrMap);
			}
		}
		maxInactiveIntervalInSeconds = enableAttrs.getNumber("maxInactiveIntervalInSeconds");
	}

	@Autowired(required = false)
	public void setHttpSessionStrategy(HttpSessionStrategy httpSessionStrategy) {
		this.httpSessionStrategy = httpSessionStrategy;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.BeanClassLoaderAware#setBeanClassLoader(java.lang.ClassLoader)
	 */
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}
}
