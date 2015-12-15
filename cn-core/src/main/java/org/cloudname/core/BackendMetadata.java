package org.cloudname.core;

import java.util.function.Function;

/**
 * Metadata about the backends. Implement this class and reference it in
 * <pre>src/main/resources/META-INF/services/org.cloudname.core.BackendMetadata</pre>
 *
 * <p>Backends are created via the @link{BackendManager} class which reads the registered
 * metatadata classes. The backend URL provided by the clients is formatted like
 * <pre>[backend]://[connection string]</pre>
 *
 * @author stalehd@gmail.com
 */
public interface BackendMetadata {
    /**
     * The name of the backend.
     */
    String getName();

    /**
     * A factory method for the backend. The method takes one parameter - a connection string.
     */
    Function<String, CloudnameBackend> getFactoryMethod();
}
