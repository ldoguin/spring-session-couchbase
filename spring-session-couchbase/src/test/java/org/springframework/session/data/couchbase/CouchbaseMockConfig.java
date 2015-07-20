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
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.couchbase.mock.CouchbaseMock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.session.data.couchbase.config.annotation.web.http.EnableCouchbaseHttpSession;

@EnableCouchbaseHttpSession
@Configuration
public class CouchbaseMockConfig {
    private CouchbaseMock mock;
    @Bean
    public AsyncBucket asyncBucket() throws Exception {
        mock = new CouchbaseMock("127.0.0.1", 8091, 1,11210, 1, null, 1);
        mock.start();
        mock.waitForStartup();
        int port = mock.getBuckets().get("default").activeServers().get(0).getPort();
        CouchbaseEnvironment ce = DefaultCouchbaseEnvironment.builder().bootstrapCarrierDirectPort(port).build();
        CouchbaseCluster cluster = CouchbaseCluster.create(ce, "127.0.0.1");
        return cluster.openBucket("default").async();
    }

}