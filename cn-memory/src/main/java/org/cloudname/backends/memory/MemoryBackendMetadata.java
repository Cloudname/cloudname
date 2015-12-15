package org.cloudname.backends.memory;

import org.cloudname.core.BackendMetadata;
import org.cloudname.core.CloudnameBackend;

import java.util.function.Function;

/**
 * Metadata for the memory backend
 *
 * @author stalehd@gmail.com
 */
public class MemoryBackendMetadata implements BackendMetadata {
    @Override
    public String getName() {
        return "memory";
    }

    @Override
    public Function<String, CloudnameBackend> getFactoryMethod() {
        return (connectionString) -> new MemoryBackend();
    }
}
