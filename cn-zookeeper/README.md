# ZooKeeper backend

# Node structure
The root path is set to `/cn` and the leases are stored in `/cn/temporary` and `/cn/permanent`. Temporary leases use ephemeral nodes with a randomly assigned 4-byte long ID. Permanent leases are named by the client. The Curator library is used for the majority of ZooKeeper access. The containing nodes have the `CONTAINER` bit set, i.e. they will be cleaned up by ZooKeeper when there's no more child nodes inside each of the containers. Note that this feature is slated for ZooKeeper 3.5 which is currently in Alpha (as of November 2015). Until then the Curator library uses regular nodes so if it is deployed on a ZooKeeper 3.4 or lower manual cleanups of nodes is necessary.
