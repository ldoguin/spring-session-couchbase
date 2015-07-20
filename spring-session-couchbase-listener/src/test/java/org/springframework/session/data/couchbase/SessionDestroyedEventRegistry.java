package org.springframework.session.data.couchbase;

import org.springframework.context.ApplicationListener;
import org.springframework.session.events.SessionDestroyedEvent;

/**
 * Created by ldoguin on 20/07/15.
 */
public class SessionDestroyedEventRegistry implements ApplicationListener<SessionDestroyedEvent> {
    private boolean receivedEvent;
    private Object lock;

    public void onApplicationEvent(SessionDestroyedEvent event) {
        synchronized (lock) {
            receivedEvent = true;
            lock.notifyAll();
        }
    }

    public boolean receivedEvent() {
        return receivedEvent;
    }

    public void setLock(Object lock) {
        this.lock = lock;
    }
}
