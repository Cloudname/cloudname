Cloudname A3
============

A3 is an authentication and authorization library. You can use it
standalone or benefit from its integration with Jersey to secure
your REST services.

Currently it uses a file-based user database where each user has
name, password, role(s), custom properties and more.

Standalone use
--------------

See `/a3/src/test/java/org/cloudname/a3/A3ClientTest.java` for examples.

Use with Jersey
---------------

A3 in combination with Jersey does:

* Authentication of users for resources that require it (anonymous access to
  non-secured resources is permitted)
* Sending an authentication challenge to users if they try to access a secured
  resource without providing authentication credentials
* Support for role-based authorization with the JSR-250 annotations such as
  `@RolesAllows`

### Authentication

Register the `org.cloudname.a3.jaxrs.JerseyRequestFilter` with Jersey
to enable authentication (see its JavaDoc for instructions). It requires
you to create a JAX-RS/Jersey `@Provider` to supply the A3Client instance
to use.

You should also register the package `org.cloudname.a3.jaxrs` with Jersey
so that it will find the `AuthenticationExceptionMapper` @Provider - it takes
care of translation authentication errors into the right HTTP errors and of
requesting authentication from the user if it is required and not provided.
(Include the package among those in the init parameter `com.sun.jersey.config.property.packages`.)

### Role-Based Authorization with @RolesAllowed etc.

To enable role-based authorization, register the filter factory
`org.cloudname.a3.jaxrs.JerseyRoleBasedAccessControlResourceFilterFactory` with Jersey,
see JavaDoc in the class for instructions.

Other Stuff
-----------

### Editing a User Database File

You can use the command-line utility `org.cloudname.a3.editor.Editor`
to create or edit a user database file for A3.
