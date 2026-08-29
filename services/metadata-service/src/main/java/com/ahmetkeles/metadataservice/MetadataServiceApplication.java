package com.ahmetkeles.metadataservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.security.Security;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class MetadataServiceApplication {

    public static void main(String[] args) {
        // The JDK caches FAILED host lookups for 10s (a security property
        // set in java.security, so the sun.net.inetaddr.negative.ttl system
        // property cannot override it). This coordinator keeps resolving
        // storage-node names while a node is down — with that cache, a node
        // coming back stays invisible for up to 10 more seconds after it is
        // healthy, failing uploads placed on it. A returned node must be
        // usable the moment its name resolves again.
        Security.setProperty("networkaddress.cache.negative.ttl", "0");

        SpringApplication.run(MetadataServiceApplication.class, args);
    }
}
