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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.cluster.*;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.message.dcp.StreamRequestResponse;
import com.couchbase.client.deps.com.lmax.disruptor.EventTranslatorOneArg;
import com.couchbase.client.deps.com.lmax.disruptor.RingBuffer;
import org.springframework.util.Assert;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link CouchbaseReader} is in charge of accepting events from Couchbase.
 *
 * @author Sergey Avseyev
 */
public class CouchbaseReader {
    private final ClusterFacade core;
    private final RingBuffer<DCPEvent> dcpRingBuffer;
    private final List<String> nodes;
    private final String bucket;
    private final String streamName;
    private final String password;


    private static final EventTranslatorOneArg<DCPEvent, CouchbaseMessage> TRANSLATOR =
            new EventTranslatorOneArg<DCPEvent, CouchbaseMessage>() {
                @Override
                public void translateTo(final DCPEvent event, final long sequence, final CouchbaseMessage message) {
                    event.setMessage(message);
                }
            };
    /**
     *
     * @param core the core reference.
     * @param dcpRingBuffer the buffer where to publish new events.
     * @param nodes the list of Couchbase nodes.
     * @param bucket the name of source bucket.
     * @param password the bucket password.
     */
    public CouchbaseReader(final ClusterFacade core, final RingBuffer<DCPEvent> dcpRingBuffer, final List<String> nodes,
                           final String bucket, final String password) {
        this.core = core;
        this.dcpRingBuffer = dcpRingBuffer;
        this.nodes = nodes;
        this.bucket = bucket;
        this.password = password;
        this.streamName = "CouchbaseKafka(" + this.hashCode() + ")";
    }

    /**
     * Performs connection with 2 seconds timeout.
     */
    public void connect() {
        connect(2, TimeUnit.SECONDS);
    }

    /**
     * Performs connection with arbitrary timeout
     *
     * @param timeout the custom timeout.
     * @param timeUnit the unit for the timeout.
     */
    public void connect(final long timeout, final TimeUnit timeUnit) {
        core.send(new SeedNodesRequest(nodes))
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();
        core.send(new OpenBucketRequest(bucket, password))
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();

    }

    public void disconnect() {
        disconnect(2, TimeUnit.SECONDS);
    }

    /**
     * Performs connection with arbitrary timeout
     *
     * @param timeout the custom timeout.
     * @param timeUnit the unit for the timeout.
     */
    public void disconnect(final long timeout, final TimeUnit timeUnit) {
        CouchbaseResponse dr = core.send(new DisconnectRequest())
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();
        Assert.isTrue(dr.status().isSuccess(), "Disconnected");



    }

    /**
     * Executes worker reading loop, which relays events from Couchbase to Kafka.
     */
    public void run() {
        core.send(new OpenConnectionRequest(streamName, bucket))
                .toList()
                .flatMap(new Func1<List<CouchbaseResponse>, Observable<Integer>>() {
                    @Override
                    public Observable<Integer> call(final List<CouchbaseResponse> couchbaseResponses) {
                        return partitionSize();
                    }
                })
                .flatMap(new Func1<Integer, Observable<DCPRequest>>() {
                    @Override
                    public Observable<DCPRequest> call(final Integer numberOfPartitions) {
                        return requestStreams(numberOfPartitions);
                    }
                })
//                .toBlocking()
                .forEach(new Action1<DCPRequest>() {
                    @Override
                    public void call(final DCPRequest dcpRequest) {
                        dcpRingBuffer.tryPublishEvent(TRANSLATOR, dcpRequest);
                    }
                });

    }


    private Observable<Integer> partitionSize() {
        return core
                .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .map(new Func1<GetClusterConfigResponse, Integer>() {
                    @Override
                    public Integer call(final GetClusterConfigResponse response) {
                        CouchbaseBucketConfig config = (CouchbaseBucketConfig) response
                                .config().bucketConfig(bucket);
                        return config.numberOfPartitions();
                    }
                });
    }

    private Observable<DCPRequest> requestStreams(final int numberOfPartitions) {
        return Observable.merge(
                Observable.range(0, numberOfPartitions)
                        .flatMap(new Func1<Integer, Observable<StreamRequestResponse>>() {
                            @Override
                            public Observable<StreamRequestResponse> call(final Integer partition) {
                                return core.send(new StreamRequestRequest(partition.shortValue(), bucket));
                            }
                        })
                        .map(new Func1<StreamRequestResponse, Observable<DCPRequest>>() {
                            @Override
                            public Observable<DCPRequest> call(final StreamRequestResponse response) {
                                return response.stream();
                            }
                        })
        );
    }
}