Sample Cloudname-Based REST Application
=======================================

A minimalistic REST application based on Cloudname/Base and an embedded Jetty.

To be used as a starting point for your REST applications.

TODO
----

* Upgrade to Base 3.6, use its embedded Jetty (replace WebServer with its com.comoyo.jetty.JettyServer)
* Move this to a standlone sample repo, e.g. "comoyo-sample-code" together with https://github.com/comoyo/javasamlsp
* Clarify clean way to shut down Base - see Markus' comment on the Base page (B. 3.5)
* ...

Bakksjø: I think the original implementors of base just added a shutdown() method so that you can do a clean shutdown of base. This is useful in tests, for instance.
If your JVM is going down anyway, I don't think it's critical that base.shutdown() is called before exit.
Also, shutdown hooks are not a good way to do a clean shutdown (IMO). It's sort of like Thread.stop(), which the Java language designers discovered was a bad idea. What you do want instead is to have code that explicitly takes things down controlled, in the right order - typically the opposite of the initialization order. You don't add "initialization hooks", so why should you add shutdown hooks?
Markus: Calling Base.shutdown() is necessary to ensure that logs are flushed before the application stops.

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
