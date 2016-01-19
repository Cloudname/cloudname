package org.cloudname.a3.domain;

/**
 * This class provides a service coordinate.  A service coordinate consists
 * of a datacenter id, a username, a service name and an instance id.
 *
 * @author borud
 */
public class ServiceCoordinate {
    private String datacenter;
    private String user;
    private String serviceName;
    private int instance;

    /**
     * Builder for ServiceCoordinate.
     */
    public static class Builder {
        private String datacenter;
        private String user;
        private String serviceName;
        private int instance;

        public Builder setDatacenter(String datacenter) {
            this.datacenter = datacenter;
            return this;
        }

        public Builder setUser(String user) {
            this.user = user;
            return this;
        }

        public Builder setServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder setInstance(int instance) {
            this.instance = instance;
            return this;
        }

        public ServiceCoordinate build() {
            return new ServiceCoordinate(datacenter, user, serviceName, instance);
        }
    }

    /**
     * Create an immutable Service coordinate.
     *
     * @param datacenter The datacenter where the service lives
     * @param user The user that owns the service
     * @param serviceName The name of the service
     * @param instance The instance number of the service
     */
    public ServiceCoordinate(String datacenter, String user, String serviceName, int instance) {
        this.datacenter = datacenter;
        this.user = user;
        this.serviceName = serviceName;
        this.instance = instance;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public String getUser() {
        return user;
    }

    public String getServiceName() {
        return serviceName;
    }

    public int getInstance() {
        return instance;
    }

    /**
     * Create a path prefix for a given namespace.
     *
     * @param namespace The namespace for the path prefix.  Examples: "acl", "cn" etc.
     * @param includeInstance if {@code true} we include the instance part of the path (eg "/2")
     * @return a ZooKeeper path prefix
     */
    public String getPathPrefix(String namespace, boolean includeInstance) {
        StringBuilder buffer = new StringBuilder()
                .append(namespace)
                .append("/")
                .append(datacenter)
                .append("/")
                .append(user)
                .append("/")
                .append(serviceName);

        if (includeInstance) {
            buffer.append("/").append(instance);
        }
        return buffer.toString();
    }

    /**
     * Get the path prefix for a Service coordinate.
     *
     * @param namespace THe namespace for the path prefix
     * @return a ZooKeeper path prefix
     */
    public String getPathPrefix(String namespace) {
        return getPathPrefix(namespace, false);
    }

    @Override
    public String toString() {
        return "ServiceCoordinate{" +
                "datacenter='" + datacenter + '\'' +
                ", user='" + user + '\'' +
                ", serviceName='" + serviceName + '\'' +
                ", instance=" + instance +
                '}';
    }
}
