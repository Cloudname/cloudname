# Cloudname Core

The core libraries are mostly for internal use and are the basic building block for the other libraries. Clients won't use or access this library directly but through the libraries built on the core library.

The core library supports various backends. The built-in backend is memory-based and is *not* something you want to use in a production service. Its sole purpose is to provide a fast single-JVM backend used when testing other modules built on top of the core library.

## Key concepts
### Leases
The backends expose **leases** to clients. Each lease is represented by a **path**. Clients belong to a **region**. A region is typically a cluster of servers that are coordinated through a single backend.



#### Temporary leases
Temporary leases exist only as long as the client is running and is connected to the backend. When the client terminates the connection the lease expires and anyone listening on changes will be notified.

#### Permanent leases
Permanent leases persist between client connections. If a client connects to the backend, creates a permanent lease and then disconnects the lease will still be in place. The permanent leases do not expire and will only be removed if done so explicitly by the clients.

### Paths
A **path** is nothing more than an ordered set of strings that represents a (real or virtual) tree structure. The backend itself does not need to use a hierarchical storage mechanism since the paths can be used directly as identifiers.

Elements in the paths follow the DNS naming conventions in RFC 952 and RFC 1123: Strings between 1-63 characters long, a-z characters (case insensitive) and hyphens. A string cannot start or end with a hyphen.


## Backend requirements
* Paths are guaranteed unique for all clients in the same cluster. There is no guarantee that a lease will be unique for other regions.
* The backend ensures there are no duplicate leases for the current region.
* The backend will create notifications in the same order as they occur.
* Past leases given to disconnected clients are not guaranteed to be unique
* The backend is responsible for cleanups of leases; if all clients disconnect the only leases that should be left is the permanent leases.

## Implementing new backends
New implementations must implement two ìnterfaces -- CloudnameBackend` and `BackendMetadata. Each backend is registered with the `org.cloudname.core.BackendManager class which uses the ServiceLoader class to locate the implementation.

Implement the BackendMetadata interface in a class and put the class name of the class that implements BackendMetadata into a file named `src/main/resources/META-INF/services/org.cloudname.core.BackendMetadata`. As soon as the jar file is included in the classpath of the client the backend will become available for the client to use. Needless to say, the name should be unique but that shouldn't be an issue. There are no restrictions on the connection string the backend can use. Everything after the `[backend]://` is passed on unaltered to the backend.

Note that the BackendMetadata isn't required for a working backend but strongly recommended. It is easier to switch backends for testing environments, experimentation if the backend can be created via the `BackendManager` class.