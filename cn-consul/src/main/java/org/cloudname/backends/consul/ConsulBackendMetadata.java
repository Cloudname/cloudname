package org.cloudname.backends.consul;

import org.cloudname.core.BackendMetadata;
import org.cloudname.core.CloudnameBackend;

import java.util.function.Function;

/**
 * The Consul backend's metadata
 */
public class ConsulBackendMetadata implements BackendMetadata {
    @Override
    public String getName() {
        return "consul";
    }

    @Override
    public Function<String, CloudnameBackend> getFactoryMethod() {
        return ConsulBackend::new;
    }
}
