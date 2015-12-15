# Cloudname service discovery

## Coordinates
Each service that runs is represented by a **coordinate**. There are two kinds of coordinates:
* **Service coordinates** which are generic coordinates that points to one or more services
* **Instance coordinates** which points to a particular service

Coordinates are specified through **regions** and **tags**. A **region** is a separate (logical) cluster of services. One region is usually not connected to another region. The simplest comparison is either a *data center* or an AWS *region* or *availability zone* (like eu-west-1, us-east-1 and so on).

The **tag** is just that - a tag that you can assign to a cluster of different services. The tag doesn't contain any particular semantics.

A **service coordinate** looks like `<service>.<tag>.<region>`, f.e. `geolocation.rel1501.dc1` or (if you are running in AWS and have decided that you'll assume regions are availability zones) `geolocation.rel1501.eu-west-1a`.

Instance coordinates points to a particular service instance and looks like this: `<instance identifier>.<service name>.<tag>.<region>`. For the examples above the instance coordinates might look like `ff08f0ah.geolocation.rel1501.dc1` or `ab08bed5.geolocation.rel1501.eu-west-1a`.

The instance identifier is an unique identifier for that instance. Note that the instance identifier isn't unique across all services, isn't sequential and does not carry any semantic information.

## Register a service
A service is registered through the `CloudnameService` class:
```java
// Create the service class. Note that getBackend() returns a Cloudname backend
// instance. There ar multiple types available.
try (CloudnameService cloudnameService = new CloudnameService(getBackend())) {
    // Create the coordinate and endpoint
    ServiceCoordinate serviceCoordinate = ServiceCoordinate.parse("myservice.demo.local");
    Endpoint httpEndpoint = new Endpoint("http", "127.0.0.1", 80);

    ServiceData serviceData = new ServiceData(Arrays.asList(httpEndpoint));

    // This will register the service. The returned handle will expose the registration
    // to other clients until it is closed.
    try (ServiceHandle handle = cloudnameService.registerService(serviceCoordinate, serviceData)) {

        // ...Run your service here

    }    
}
```

## Looking up services
Services can be located without registering a service; supply a listener to the CloudnameService instance to get notified of new services:
```java
CloudnameService cloudnameService = new CloudnameService(getBackend());
ServiceCoordinate serviceCoordinate = ServiceCoordinate.parse("myservice.demo.local");
cloudnameService.addServiceListener(ServiceCoordinate, new ServiceListener() {
    @Override
    public void onServiceCreated(final InstanceCoordinate coordinate, final ServiceData data) {
        // A new instance is launched. Retrieve the endpoints via the data parameter.
        // Note that this method is also called when the listener is set so you'll
        // get notifications on already existing services as well.
    }

    @Override
    public void onServiceDataChanged(final InstanceCoordinate coordinate, final ServiceData data) {
        // There's a change in endpoints for the given instance. The updated endpoints
        // are supplied in the data parameter
    }

    @Override
    public void onServiceRemoved(final InstanceCoordinate coordinate) {
        // One of the instances is stopped. It might become unavailable shortly
        // (or it might have terminated)
    }
});
```

## Permanent services
Some resources might not be suitable for service discovery, either because they are not under your control, they are pet services or not designed for cloud-like behavior (aka "pet servers"). You can still use those in service discovery; just add them as *permanent services*. Permanent services behave a bit differently from ordinary services; they stay alive for long periods of time and on some rare occasions they change their endpoint. Registering permanent services are similar to ordinary services. The following snippet registers a permanent service, then terminates. The service registration will still be available to other clients when this client has terminated:

```java
try (CloudnameService cloudnameService = new CloudnameService(getBackend())) {
    ServiceCoordinate coordinate = ServiceCoordinate.parse("mydb.demo.local");
    Endpoint endpoint = new Endpoint("db", "127.0.0.1", 5678);

    if (!cloudnameService.createPermanentService(coordinate, endpoint)) {
        System.out.println("Couldn't register permanent service!");
    }    
}
```
Note that permanent services can not have more than one endpoint registered at any time. A permanent service registration applies only to *one* service at a time.

Looking up permanent service registrations is similar to ordinary services:

```java
try (CloudnameService cloudnameService = new CloudnameService(getBackend())) {
    ServiceCoordinate coordinate = ServiceCoordinate.parse("mydb.demo.local");
    cloudnameService.addPermanentServiceListener(coordinate,
        new PermanentServiceListener() {
            @Override
            public void onServiceCreated(Endpoint endpoint) {
                // Service is created. Note that this is also called when the
                // listener is set so you'll get notifications on already
                // existing services as well.
            }

            @Override
            public void onServiceChanged(Endpoint endpoint) {
                // The endpoint is updated
            }

            @Override
            public void onServiceRemoved() {
                // The service has been removed
            }
        });
}
```
