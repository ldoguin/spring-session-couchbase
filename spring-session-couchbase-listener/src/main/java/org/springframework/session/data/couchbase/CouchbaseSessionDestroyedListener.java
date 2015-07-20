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

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.deps.com.lmax.disruptor.EventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.session.events.SessionDestroyedEvent;
import org.springframework.util.Assert;

/**
 *
 * @author ldoguin
 */
public class CouchbaseSessionDestroyedListener implements EventHandler<DCPEvent> {
    private static final Log logger = LogFactory.getLog(CouchbaseSessionDestroyedListener.class);

    private final ApplicationEventPublisher eventPublisher;

    /**
     * @param eventPublisher the {@link ApplicationEventPublisher} to use. Cannot be null.
     */
    public CouchbaseSessionDestroyedListener(ApplicationEventPublisher eventPublisher) {
        Assert.notNull(eventPublisher, "eventPublisher cannot be null");
        this.eventPublisher = eventPublisher;
    }

    /**
     * Handles {@link DCPEvent}s that come into the response RingBuffer.
     */
    @Override
    public void onEvent(final DCPEvent event, final long sequence, final boolean endOfBatch) throws Exception {
        if (pass(event)) {
            String sessionId = event.key();
            if(logger.isDebugEnabled()) {
                logger.debug("Publishing SessionDestroyedEvent for session " + sessionId);
            }
            publishEvent(new SessionDestroyedEvent(this, sessionId));
        }
    }

    public boolean pass(final DCPEvent dcpEvent) {
        if (dcpEvent.key().startsWith(CouchbaseSession.SESSION_KEY_PREFIX)) {
            return dcpEvent.message() instanceof MutationMessage
                    || dcpEvent.message() instanceof RemoveMessage;
        }
        return false;
    }

    private void publishEvent(ApplicationEvent event) {
        try {
            this.eventPublisher.publishEvent(event);
        }
        catch (Throwable ex) {
            logger.error("Error publishing " + event + ".", ex);
        }
    }
}
