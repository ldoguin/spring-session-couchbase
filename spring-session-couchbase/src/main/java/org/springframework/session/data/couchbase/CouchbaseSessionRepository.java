/*
 * Copyright 2002-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
* the License.
 */
package org.springframework.session.data.couchbase;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.SerializableDocument;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.session.SessionRepository;
import org.springframework.util.Assert;
import rx.Observable;
import rx.functions.Func1;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link org.springframework.session.SessionRepository} for
 * {@link org.springframework.session.data.couchbase.CouchbaseSession}. It's backed by a couchbase bucket.
 *
 * <p>
 * The implementation does NOT support firing {@link org.springframework.session.events.SessionDestroyedEvent}.
 * </p>
 *
 * @author Laurent Doguin
 * @since 1.0
 */
public class CouchbaseSessionRepository implements SessionRepository<CouchbaseSession> {

    public static final String CREATION_TIME_ATTR = "creationTime";

    public static final String MAX_INACTIVE_ATTR = "maxInactiveInterval";

    public static final String LAST_ACCESSED_ATTR = "lastAccessedTime";

    /**
	 * If non-null, this value is used to override {@link org.springframework.session.ExpiringSession#setMaxInactiveIntervalInSeconds(int)}.
	 */
	private Integer defaultMaxInactiveInterval;

	private final AsyncBucket asyncBucket;

    public CouchbaseSessionRepository(AsyncBucket asyncBucket) {
        Assert.notNull(asyncBucket, "connectionFactory cannot be null");
        this.asyncBucket = asyncBucket;
    }

	/**
	 * If non-null, this value is used to override {@link org.springframework.session.data.couchbase.CouchbaseSession#setMaxInactiveIntervalInSeconds(int)}.
	 * @param defaultMaxInactiveInterval the number of seconds that the {@link org.springframework.session.Session} should be kept alive between client requests.
	 */
	public void setDefaultMaxInactiveInterval(int defaultMaxInactiveInterval) {
		this.defaultMaxInactiveInterval = Integer.valueOf(defaultMaxInactiveInterval);
	}

	public void save(CouchbaseSession session) {
        JsonDocument sd = JsonDocument.create(
                session.getId(), session.getMaxInactiveIntervalInSeconds(), session.getSessionAttrs());
        asyncBucket.upsert(sd).toBlocking().single();
	}

	public CouchbaseSession getSession(String id) {
        return asyncBucket.get(id).singleOrDefault(null).map(
                new Func1<JsonDocument, CouchbaseSession>() {
                    @Override
                    public CouchbaseSession call(JsonDocument jd) {
                        if (jd == null) return null;
                        CouchbaseSession session = new CouchbaseSession(jd);
                        session.setLastAccessedTime(System.currentTimeMillis());
//                        asyncBucket.touch(jd).subscribe();
                        return session;
                    }
                }).toBlocking().single();
	}

    public void delete(String id) {
		asyncBucket.remove(id).toBlocking().single();
	}

	public CouchbaseSession createSession() {
		CouchbaseSession result = new CouchbaseSession();
		if(defaultMaxInactiveInterval != null) {
			result.setMaxInactiveIntervalInSeconds(defaultMaxInactiveInterval);
		}
		return result;
	}
}
