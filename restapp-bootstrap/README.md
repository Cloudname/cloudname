Sample Cloudname-Based REST Application
=======================================

A minimalistic REST application based on Cloudname/Base and an embedded Jetty.

To be used as a starting point for your REST applications.

Structure
---------

Key sub-packages:

* rs - JAX-RS resources
* server - the infrastructure code integrating with Jetty and Base,
  providing security and enabling starting from the command line


Using the App: Develop, Deploy, Configure, Run
-----------------------------------------------

### Development

Run the application during development:

    mvn -o compile exec:java -Dexec.mainClass=org.cloudname.example.restapp.server.Main -Dexec.args="--coordinate 0.dummy.service.coordinate"

Tip: Consider purchasing JRebel to enable restart-less development.

### Build And Run

Building and running the application for/in production:

    mvn package; java -jar target/<project>-<version>-jar-with-dependencies.jar <arguments>

(Where arguments contain at least --coordinate with a valid value.)

### Configuring the webapp

Most configuration is passed to the webapp as command-line parameters
(usually stored in a file used when starting the app).

### Running The App As a Linux Service

TBD (/etc/defaults/xxx, /etc/init.d/xxx, Puppet config ?)

Authentication and Authorization
--------------------------------

You can use the JSR-250 annotations such as `@RolesAllowed` to limit access to a service
(see the ExampleSecuredResource).

The username and password is verified using Cloudname A3.

Customizing the App
-------------------

* Rename the ExampleResource[IT] and root packages to whatever names are suitable for you
* Set org.cloudname.example.restapp.server.Main.SERVICE_NAME
* Set org.cloudname.example.restapp.server.WebServer.REST_RESOURCE_PACKAGES
* Set the mainClass parameter of the maven-jar-plugin in pom.xml

Further Info
------------

### Services provided by Base

* Processing of command-line arguments and storing their values into class fields
* Registration with the central service registry (ZooKeeper) so that other services can find it
* Support for monitoring
* Logging to the central log server
