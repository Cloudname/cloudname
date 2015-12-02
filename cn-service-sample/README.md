# The Pac Sample

The sample uses ZooKeeper as the backend. The main demo server (the Pac Server) monitors the services and displays the running services in a web page. The web page is updated automatically through a web socket whenever there's a change in the services.

The demo services (aka "the ghosts") registers in Cloudname via the service discovery API, monitors for other services with the same name and idles. Each demo service creates one endpoint to shut it down. The port is allocated dynamically.

Click on a ghost to see its internal state (note that this requires access to the service's endpoint). Click on the shutdown button to shut down the service gracefully.

# Running in Docker
You'll need three different Docker images: `cloudname/cn-zookeeper`, `cloudname/pacserver` and `cloudname/ghost`. The images are published on Docker Hub so you do not need to build them.

First launch the ZooKeeper image (assuming you use the `default` docker-machine):
```
eval $(docker-machine env default)
export DOCKER_IP=$(docker-machine ip default)
docker run -d --net=host cloudname/cn-zookeeper
docker run -d --net=host -e zookeeper=$DOCKER_IP:2181 cloudname/pacserver
```
Open up your browser at http://$DOCKER_IP:4567/ to see the service.

Then launch a ghost named *blinky*:
```
docker run -d --net=host -e zookeeper=$DOCKER_IP:2181 -e ghost=blinky -e cloudname_interface=$DOCKER_IP cloudname/ghost
```

The ghost should appear on the pac server's web page. Launch a few more ghosts:
```
docker run -d --net=host -e zookeeper=$DOCKER_IP:2181 -e ghost=inky -e cloudname_interface=$DOCKER_IP cloudname/ghost
docker run -d --net=host -e zookeeper=$DOCKER_IP:2181 -e ghost=pinky -e cloudname_interface=$DOCKER_IP cloudname/ghost
docker run -d --net=host -e zookeeper=$DOCKER_IP:2181 -e ghost=clyde -e cloudname_interface=$DOCKER_IP cloudname/ghost
```

The new ghosts should appear on the web page. Click on one of the ghosts to see its internal state. Click on the *Shutdown* button to shut it down. It should disappear from the web page.

(If you can't be bothered to do all of this you can see it in action at https://youtu.be/dlrNYbGZvAk)
