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


import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.dcp.MutationMessage;

/**
 * A pre allocated event which carries a {@link CouchbaseMessage} and associated information.
 *
 * @author Sergey Avseyev
 */

public class DCPEvent {
    /**
     * Current message from the stream.
     */
    private CouchbaseMessage message;

    /**
     * Set the new message as a payload for this event.
     *
     * @param message the message to override.
     * @return the {@link DCPEvent} for method chaining.
     */
    public DCPEvent setMessage(final CouchbaseMessage message) {
        this.message = message;
        return this;
    }

    /**
     * Get the mesage from the payload.
     *
     * @return the actual message.
     */
    public CouchbaseMessage message() {
        return message;
    }

    /**
     * Extract key from the payload.
     *
     * @return the key of message or null.
     */
    public String key() {
        if (message instanceof MutationMessage) {
            MutationMessage mutation = (MutationMessage) message;
            return mutation.key();
        } else {
            return null;
        }
    }
}