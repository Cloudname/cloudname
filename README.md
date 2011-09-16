Cloudname
==========

This is just getting started.  We'll have one subdirectory per subproject,
one for global documentation and one for the license.  Right now we have
nothing, please change that as soon as you can :-) (Rmz)


Borud had these initial comments about how to organize the github
presence:


Go right ahead :-).

I am somewhat uncertain about how it should be structured on GitHub since
it is going to consist of a bunch of libraries: There's Cloudname the
project and there's Cloudname the cloud service coordination library .  It
would probably make sense to start with just the Cloudname library proper.
The main parts so far

- Cloudname - Java library for cloud service coordination.  Initially solves naming, resolution.  Will also have a command line client for management.
- Cloudname Base - Housekeeping for cloud services.  Provides simple monitoring, built in HTTP interface and eventually tools for inspecting state and affecting runtime state of logging etc.
- Cloudname Bjorn - Per node manager of artifacts and services (Python, C or C++?).  Wordplay invented by Ståle.  As in Bjorn Borg (equivalent to Borglet)
- Cloudname BucketManager - Dynamic shard management.
- Cloudname DNS - DNS server that lives atop
- Cloudname A3 - Simple lightweight AAA for services.
- Cloudname Idgen - System for effective generation of unique IDs
- Cloudname Timber - Log server
- Cloudname Test - Tools needed for testing.

Cloudname, BucketManager and DNS do not exist yet.  A prototype API exists
for parts of Cloudname. The BucketManager has been mostly figured out, but
not started.  DNS has not been started and not been thought about that
much yet (Arnt expressed an interest in writing it.  I'd be interested in
seeing if a DNS server could be done using Netty).

Appbase, A3, Idgen and Timber exist as started projects. The initial
checkin on Appbase was this sunday by me :).  A3 and IdGen have enough
functionality to be useful right now, but nothing more.  Timber currently
implements the basics of a log server -- what is missing is the client
parts (which I will be working on this week).

Cloudname Test exists in the form of common/test-tools.  We also need to
market them a bit better internally because I don't think most people are
aware of their existence.  Cloudname Test is critical to develop stuff for
ZooKeeper since it contains tools for embedding ZK server in tests.  (It
must be possible to build any and all artifacts on a clean machine without
requiring any particular servers to be running).

We also need a public Maven Repository for the short term.
http://cemerick.com/2010/08/24/hosting-maven-repos-on-github/ describes an
easy-looking way to do that on github. In the long term we may want to get
our artifacts into the Maven Central repository servers.

But first things first.  I propose we start with Cloudname and the first
thing that needs to be done there is to check in the API.  I'll have a
look at what we've got.  (We made a version 1 API prototype, but the
design we went for was too clumsy.  The design we came up with in
march/april is much more suitable.  But there are still important
questions).
