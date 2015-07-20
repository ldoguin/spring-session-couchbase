package org.springframework.session.data.couchbase;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.deps.com.lmax.disruptor.ExceptionHandler;
import com.couchbase.client.deps.com.lmax.disruptor.RingBuffer;
import com.couchbase.client.deps.com.lmax.disruptor.dsl.Disruptor;
import com.couchbase.client.deps.io.netty.util.concurrent.DefaultThreadFactory;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.CouchbaseCluster;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.session.data.couchbase.config.annotation.web.http.EnableCouchbaseHttpSession;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by ldoguin on 20/07/15.
 */
@EnableCouchbaseHttpSession
@Configuration
class CouchbaseConfig {

    private static final DCPEventFactory DCP_EVENT_FACTORY = new DCPEventFactory();
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseConfig.class);

    @Autowired
    ApplicationEventPublisher eventPublisher;

    private ClusterFacade core;
    private ExecutorService disruptorExecutor;
    private Disruptor<DCPEvent> disruptor;
    private RingBuffer<DCPEvent> dcpRingBuffer;
    private CouchbaseReader couchbaseReader;


    @Bean
    public SessionDestroyedEventRegistry sessionDestroyedEventRegistry() {
        return new SessionDestroyedEventRegistry();
    }

    @Bean
    public AsyncBucket asyncBucket() throws Exception {

        DefaultCoreEnvironment ce = DefaultCoreEnvironment.builder().dcpEnabled(true).build();
        core = new CouchbaseCore(ce);
        CouchbaseCluster cluster = CouchbaseCluster.create();
        disruptorExecutor = Executors.newFixedThreadPool(2, new DefaultThreadFactory("cb-kafka", true));
        disruptor = new Disruptor<DCPEvent>(
                DCP_EVENT_FACTORY,
                64,
                disruptorExecutor
        );
        disruptor.handleExceptionsWith(new ExceptionHandler() {
            @Override
            public void handleEventException(final Throwable ex, final long sequence, final Object event) {
                LOGGER.warn("Exception while Handling DCP Events {}, {}", event, ex);
            }

            @Override
            public void handleOnStartException(final Throwable ex) {
                LOGGER.warn("Exception while Starting DCP RingBuffer {}", ex);
            }

            @Override
            public void handleOnShutdownException(final Throwable ex) {
                LOGGER.info("Exception while shutting down DCP RingBuffer {}", ex);
            }
        });
        CouchbaseSessionDestroyedListener couchbaseSessionDestroyedListener = new CouchbaseSessionDestroyedListener(eventPublisher);
        disruptor.handleEventsWith(couchbaseSessionDestroyedListener);
        disruptor.start();
        dcpRingBuffer = disruptor.getRingBuffer();
        couchbaseReader = new CouchbaseReader(core, dcpRingBuffer, Arrays.asList("127.0.0.1"), "default", "");
        couchbaseReader.connect();
        couchbaseReader.run();
        return cluster.openBucket("default").async();
    }

}
