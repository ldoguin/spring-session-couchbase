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
package org.springframework.session.data.couchbase;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = CouchbaseMockConfig.class)
@WebAppConfiguration
public class CouchbaseOperationsSessionRepositoryTests {

    @Autowired
    private AsyncBucket asyncBucket;

    @After
    public void tearDown() throws Exception {
        asyncBucket.bucketManager().toBlocking().single().flush();
    }

    @Test
	public void createSessionDefaultMaxInactiveInterval() throws Exception {
        CouchbaseSessionRepository couchbaseRepository = new CouchbaseSessionRepository(asyncBucket);
		ExpiringSession session = couchbaseRepository.createSession();
		assertThat(session.getMaxInactiveIntervalInSeconds()).isEqualTo(new MapSession().getMaxInactiveIntervalInSeconds());
	}

	@Test
	public void createSessionCustomMaxInactiveInterval() throws Exception {
        CouchbaseSessionRepository couchbaseRepository = new CouchbaseSessionRepository(asyncBucket);
		int interval = 1;
		couchbaseRepository.setDefaultMaxInactiveInterval(interval);
		ExpiringSession session = couchbaseRepository.createSession();
		assertThat(session.getMaxInactiveIntervalInSeconds()).isEqualTo(interval);
	}

	@Test
	public void saveSetAttribute() {
        CouchbaseSessionRepository couchbaseRepository = new CouchbaseSessionRepository(asyncBucket);
		String attrName = "attrName";
		CouchbaseSession session = couchbaseRepository.createSession();
		session.setAttribute(attrName, "attrValue");
		couchbaseRepository.save(session);
        session = couchbaseRepository.getSession(session.getId());
        assertThat(session.getAttribute(attrName).equals("attrValue"));

	}

	@Test
	public void saveRemoveAttribute() {
        CouchbaseSessionRepository couchbaseRepository = new CouchbaseSessionRepository(asyncBucket);
        String attrName = "attrName";
        CouchbaseSession session = couchbaseRepository.createSession();
        session.setAttribute(attrName, "attrValue");
        couchbaseRepository.save(session);
        session = couchbaseRepository.getSession(session.getId());
        assertThat(session.getAttribute(attrName).equals("attrValue"));
        session.removeAttribute(attrName);
        couchbaseRepository.save(session);
        session = couchbaseRepository.getSession(session.getId());
        assertThat(session.getAttribute(attrName)).isNull();
    }

	@Test
	public void couchbaseSessionGetAttributes() {
        CouchbaseSessionRepository couchbaseRepository = new CouchbaseSessionRepository(asyncBucket);
		String attrName = "attrName";
		CouchbaseSession session = couchbaseRepository.createSession();
		session.setAttribute(attrName, "attrValue");
		assertThat(session.getAttributeNames()).contains(attrName);
		session.removeAttribute(attrName);
		assertThat(session.getAttribute(attrName)).isNull();
	}

	@Test
	public void delete() {
        CouchbaseSessionRepository couchbaseRepository = new CouchbaseSessionRepository(asyncBucket);
		String attrName = "attrName";
        CouchbaseSession session = couchbaseRepository.createSession();
        session.setAttribute(attrName, "attrValue");
        couchbaseRepository.save(session);
        session = couchbaseRepository.getSession(session.getId());
        assertThat(session).isNotNull();
		couchbaseRepository.delete(session.getId());
        session = couchbaseRepository.getSession(session.getId());
        assertThat(session).isNull();
	}

	@Test(expected = DocumentDoesNotExistException.class)
	public void deleteNullSession() {
        CouchbaseSessionRepository couchbaseRepository = new CouchbaseSessionRepository(asyncBucket);
		String id = "abcd";
		couchbaseRepository.delete(id);
	}

	@Test
	public void getSessionNotFound() {
        CouchbaseSessionRepository couchbaseRepository = new CouchbaseSessionRepository(asyncBucket);
		String id = "abc";
		assertThat(couchbaseRepository.getSession(id)).isNull();
	}

	@Test
	public void getSessionFound() {
        CouchbaseSessionRepository couchbaseRepository = new CouchbaseSessionRepository(asyncBucket);
		String attrName = "attrName";
		CouchbaseSession expected = couchbaseRepository.createSession();
		expected.setAttribute(attrName, "attrValue");
        couchbaseRepository.save(expected);
		long now = System.currentTimeMillis();
		CouchbaseSession session = couchbaseRepository.getSession(expected.getId());
		assertThat(session.getId()).isEqualTo(expected.getId());
		assertThat(session.getAttributeNames()).isEqualTo(expected.getAttributeNames());
		assertThat(session.getAttribute(attrName)).isEqualTo(expected.getAttribute(attrName));
		assertThat(session.getCreationTime()).isEqualTo(expected.getCreationTime());
		assertThat(session.getMaxInactiveIntervalInSeconds()).isEqualTo(expected.getMaxInactiveIntervalInSeconds());
		assertThat(session.getLastAccessedTime()).isGreaterThanOrEqualTo(now);
	}

	@Test
	public void getSessionExpired() throws InterruptedException {
        CouchbaseSessionRepository couchbaseRepository = new CouchbaseSessionRepository(asyncBucket);
        CouchbaseSession expiringSession= couchbaseRepository.createSession();
        expiringSession.setMaxInactiveIntervalInSeconds(1);
        couchbaseRepository.save(expiringSession);
        Thread.sleep(1001);
		assertThat(couchbaseRepository.getSession(expiringSession.getId())).isNull();
	}

}