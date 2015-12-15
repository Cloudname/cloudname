package org.cloudname.backends.zookeeper;

import org.cloudname.core.BackendMetadata;
import org.cloudname.core.CloudnameBackend;

import java.util.function.Function;

/**
 * ZooKeeper metadata.
 */
public class ZooKeeperBackendMetadata implements BackendMetadata {
    @Override
    public String getName() {
        return "zookeeper";
    }

    @Override
    public Function<String, CloudnameBackend> getFactoryMethod() {
        return ZooKeeperBackend::new;
    }

}
