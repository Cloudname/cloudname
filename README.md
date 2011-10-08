# A word of caution.

**This project is still under initial development and thus neither APIs
not implementation are finished.  This means that right now this
software can't be used directly, but you can use it as an inspiration
or you can choose to contribute to it if you like.**

# What is it?

Cloudname is a library for managing services in a distributed
environment.  It allows services to announce their presence and state,
it allows clients to locate services and their endpoints and it
provides a simple mechanism for distributing configuration to
services.

The current implementation uses Apache ZooKeeper to do the heavy
lifting, but programmers should never have to deal directly with
ZooKeeper -- only the library interface provided by Cloudname.

# What is in the repository

#### cn

This is the Cloudname Library and the artifact that the project is
named after.  This is probably the directory you want to look at.

#### base

Various common tools and helper classes for services.  Most of the
things under this directory are are tools used by the us in our
projects.  You may find things of interest there -- or not.

#### log

This directory contains some of the tools we use for logging.

#### timber

A skeletal implementation of a simple log server.

#### testtools

Various classes that are useful when writing tests.  For instance
we have some helper classes for network related things as well as
tools for doing tests against an embedded ZooKeeper instance.


# Cloudname basics.

In the following sections we will cover some of the Cloudname basics.

## Cloudname coordinate.

In cloudname each service has a Coordinate.  A coordinate is an
abstract name which identifies an instance of a service.  The fields
of a coordinate are:

* **cell** - Roughly equivalent to a data center or cluster.  In practical
  terms a cell is defined as a collection of machines served by
  the same ZooKeeper ensemble.  Cells normally would not span
  data centers.

* **user** - The user owning the service.  This could be a real user or it
  can be a role type user.

* **service** - The name of the service.

* **instance** - An integer between 0 and 2^31-1 identifying the instance
  number.

The *canonical* form of a coordinate is the dot form of the
coordinate.  That is, the above mentioned fields separated by dots
almost like a hostname:

>  0.myservice.borud.fbu

## Coordinates and services.

A Cloudname Coordinate is just an abstract name.  In itself it says
nothing about the physical location of a service or whether the
service is indeed running or not.  Before it can be used, it has to be
created, which is an operation that looks a lot like creating a
directory in a filesystem.

In order to to make use of a coordinate a service has to *claim* the
coordinate.  This is typically the first thing a service does when it
starts up.  Only one service can own a given coordinate at any given
time, so if the coordinate has already been claimed by another
process, the claim will not succeed.

When a service has successfully claimed a coordinate, it will get a
handle object which it can then use to:

* Modify its status information
* Publish and unpublish endpoints it provides
* Receive configuration and configuration updates.

If the service should terminate or crash, the endpoints will
automatically be unpublished.

## Clients and resolving

When a client wants to connect to a service it will use a *Resolver*
to look up the Cloudname Coordinate and get one or more *Endpoints*.
An Endpoint is just an object that describes the endpoint and contains
information about host, port, protocol etc.

The Resolver knows how to turn coordinates into endpoints, but it also
understands an extended syntax for coordinates.  For instance you can
use the resolver to ask for *named endpoints* (such as "httpport" or
"rpc-port"):

> httpport.1.myservice.borud.fbu

If you have a service that has multiple instances you may want to
resolve endpoints according to some *strategy*.  For instance you may
want all "httpport" endpoints for "myservice".  This is done by
replacing the *instance* part of the coordinate with a *strategy*:

> httpport.all.myservice.borud.fbu

This will make the resolver return a list of all the endpoints with
name "httpport" for "myservice".  

### Pluggable resolver strategies.

Initially we will provide a small set of fixed resolver strategies
that should cover the basics, but we intend to make resolver
strategies pluggable -- meaning that you will be able to provide your
own.  This will eventually enable you to do things like integrate with
your monitoring system so you can ask for things like "give me the
least loaded instance of service X".

### Connection loss.

If a client loses its connection to a service it will have to
re-resolve the coordinate or the coordinate expression to get a valid
endpoint.  This is because services are mobile -- you have to assume
that they can move from one physical node to another.  

For now, we are not going to offer any mechanisms for doing this
automatically since this will depend heavily on what makes sense for
your service.  We may revisit this at a later stage when we know more
about actual needs.

## Configuration

Cloudname will have a fairly simple mechanism for distributing
configuration to services.  There is some design work going on in this
area and we will update this section when we have landed a good
initial design.

