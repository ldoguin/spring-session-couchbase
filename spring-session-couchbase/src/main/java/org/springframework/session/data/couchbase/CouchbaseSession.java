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

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.session.ExpiringSession;
import org.springframework.session.Session;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * A {@link org.springframework.session.Session} implementation that is backed by a {@link com.couchbase.client.java.document.json.JsonObject}. The defaults for the properties are:
 * </p>
 * <ul>
 *     <li>id - a secure random generated id</li>
 *     <li>creationTime - the moment the {@link org.springframework.session.data.couchbase.CouchbaseSession} was instantiated</li>
 *     <li>lastAccessedTime - the moment the {@link org.springframework.session.data.couchbase.CouchbaseSession} was instantiated</li>
 *     <li>maxInactiveInterval - 30 minutes</li>
 * </ul>
 *
 * @since 1.0
 * @author Laurent Doguin
 */
public final class CouchbaseSession implements ExpiringSession, Serializable {

    private final static Log log = LogFactory.getLog(CouchbaseSession.class);

    public static final int THIRTY_DAYS_TIMESTAMP = 2592000;

    /**
	 * Default {@link #setMaxInactiveIntervalInSeconds(int)} (30 minutes)
	 */
    public static final int DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS = 1800;

    static final String SESSION_KEY_PREFIX = "spring:session:sessions:";

	private String id = SESSION_KEY_PREFIX + UUID.randomUUID().toString();

	private JsonObject sessionAttrs = JsonObject.create();


	/**
	 * Creates a new instance
	 */
	public CouchbaseSession() {
        long creationTime = System.currentTimeMillis();
        sessionAttrs.put(CouchbaseSessionRepository.CREATION_TIME_ATTR, creationTime);
        sessionAttrs.put(CouchbaseSessionRepository.LAST_ACCESSED_ATTR, creationTime);
        sessionAttrs.put(CouchbaseSessionRepository.MAX_INACTIVE_ATTR, DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS);
	}

	/**
	 * Creates a new instance from the provided {@link org.springframework.session.Session}
	 *
	 * @param session the {@link org.springframework.session.ExpiringSession} to initialize this {@link org.springframework.session.Session} with. Cannot be null.
	 */
	public CouchbaseSession(ExpiringSession session) {
		if(session == null) {
			throw new IllegalArgumentException("session cannot be null");
		}
		this.id = session.getId();
		this.sessionAttrs = JsonObject.create();
		for (String attrName : session.getAttributeNames()) {
			Object attrValue = session.getAttribute(attrName);
			this.sessionAttrs.put(attrName, attrValue);
		}

        sessionAttrs.put(CouchbaseSessionRepository.CREATION_TIME_ATTR, session.getCreationTime());
        sessionAttrs.put(CouchbaseSessionRepository.LAST_ACCESSED_ATTR, session.getLastAccessedTime());
        sessionAttrs.put(CouchbaseSessionRepository.MAX_INACTIVE_ATTR, session.getMaxInactiveIntervalInSeconds());
	}

	public void setLastAccessedTime(long lastAccessedTime) {
        sessionAttrs.put(CouchbaseSessionRepository.LAST_ACCESSED_ATTR, lastAccessedTime);
	}

	public long getCreationTime() {
		return sessionAttrs.getLong(CouchbaseSessionRepository.CREATION_TIME_ATTR);
	}

	public String getId() {
		return id;
	}

	public long getLastAccessedTime() {
		return sessionAttrs.getLong(CouchbaseSessionRepository.LAST_ACCESSED_ATTR);
	}

	public void setMaxInactiveIntervalInSeconds(int interval) {
        interval = convertTTLForCouchbase(interval);
		sessionAttrs.put(CouchbaseSessionRepository.MAX_INACTIVE_ATTR, interval);
	}

	public int getMaxInactiveIntervalInSeconds() {
		return sessionAttrs.getInt(CouchbaseSessionRepository.MAX_INACTIVE_ATTR);
	}

	public boolean isExpired() {
		return isExpired(System.currentTimeMillis());
	}

	boolean isExpired(long now) {
		if(getMaxInactiveIntervalInSeconds() < 0) {
			return false;
		}
		return now - TimeUnit.SECONDS.toMillis(getMaxInactiveIntervalInSeconds()) >= getLastAccessedTime();
	}

	public Object getAttribute(String attributeName) {
        Object obj = sessionAttrs.get(attributeName);
        if (obj instanceof String) {
            try {
                return stringToObject((String)obj);
            } catch (IllegalArgumentException e) {
                log.debug("Trying to deserialize but not a Serialized object: " + obj);
                return obj;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return obj;
	}

    private boolean checkType(Object item) {
        return item == null
                || item instanceof String
                || item instanceof Integer
                || item instanceof Long
                || item instanceof Double
                || item instanceof Boolean
                || item instanceof JsonObject
                || item instanceof JsonArray;
    }

    public String objectToString(Serializable object) {
        String encoded = null;
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(object);
            objectOutputStream.close();
            encoded = new String(Base64.getEncoder().encode(byteArrayOutputStream.toByteArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return encoded;
    }

    @SuppressWarnings("unchecked")
    public Object stringToObject(String string) throws IOException, ClassNotFoundException{
        if (string == null){
            return null;
        }
        byte[] bytes = Base64.getDecoder().decode(string.getBytes());
        Object object = null;
        ObjectInputStream objectInputStream = new ObjectInputStream( new ByteArrayInputStream(bytes) );
        object = objectInputStream.readObject();
        return object;
    }


    public Set<String> getAttributeNames() {
		return sessionAttrs.getNames();
	}

	public void setAttribute(String attributeName, Object attributeValue) {
		if (attributeValue == null) {
			removeAttribute(attributeName);
		} else {
            if (checkType(attributeValue)) {
                sessionAttrs.put(attributeName, attributeValue);
            } else {
                String serializedAttribute = objectToString((Serializable)attributeValue);
                sessionAttrs.put(attributeName, serializedAttribute);
            }
		}
	}

	public void removeAttribute(String attributeName) {
		sessionAttrs.removeKey(attributeName);
	}

	/**
	 * Sets the time that this {@link org.springframework.session.Session} was created in milliseconds since midnight of 1/1/1970 GMT. The default is when the {@link org.springframework.session.Session} was instantiated.
	 * @param creationTime the time that this {@link org.springframework.session.Session} was created in milliseconds since midnight of 1/1/1970 GMT.
	 */
	public void setCreationTime(long creationTime) {
		sessionAttrs.put(CouchbaseSessionRepository.CREATION_TIME_ATTR, creationTime);
	}

	/**
	 * Sets the identifier for this {@link org.springframework.session.Session}. The id should be a secure random generated value to prevent malicious users from guessing this value.   The default is a secure random generated identifier.
	 *
	 * @param id the identifier for this session.
	 */
	public void setId(String id) {
		this.id = id;
	}

	public boolean equals(Object obj) {
		return obj instanceof Session && id.equals(((Session) obj).getId());
	}

	public int hashCode() {
		return id.hashCode();
	}

	private static final long serialVersionUID = 7160779239673823561L;

    public JsonObject getSessionAttrs() {
        return sessionAttrs;
    }

    /**
     * Creates a new instance from the provided {@link org.springframework.session.Session}
     *
     * @param jsonDoc the {@link com.couchbase.client.java.document.JsonDocument} to initialize this {@link org.springframework.session.Session} with. Cannot be null.
     */
    public CouchbaseSession(JsonDocument jsonDoc) {
        if(jsonDoc == null) {
            throw new IllegalArgumentException("session cannot be null");
        }
        this.id = jsonDoc.id();
        this.sessionAttrs = JsonObject.create();
        for (String attrName : jsonDoc.content().getNames()) {
            Object attrValue = jsonDoc.content().get(attrName);
            this.sessionAttrs.put(attrName, attrValue);
        }
    }

    /**
     * Creates a new instance from the provided {@link org.springframework.session.Session}
     *
     * @param session the {@link org.springframework.session.Session} to initialize this {@link org.springframework.session.data.couchbase.CouchbaseSession} with. Cannot be null.
     */
    public CouchbaseSession(Session session) {
        if(session == null) {
            throw new IllegalArgumentException("session cannot be null");
        }
        this.id = session.getId();
        this.sessionAttrs = JsonObject.create();
        for (String attrName : session.getAttributeNames()) {
            Object attrValue = session.getAttribute(attrName);
            this.sessionAttrs.put(attrName, attrValue);
        }
    }

    /*
     * To set a value over 30 days: If you want an item to live for more than 30 days, you must provide a TTL in
     * Unix time.
     */
    public int convertTTLForCouchbase(int ttl) {
        if (ttl > THIRTY_DAYS_TIMESTAMP) {
            // Epoch
            return (int) (System.currentTimeMillis() / 1000L + ttl);
        } else {
            return ttl;
        }
    }
}