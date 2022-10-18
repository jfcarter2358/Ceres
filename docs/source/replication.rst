*******************
Replicating CeresDB
*******************

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Leader-Follower Replication
===========================

To enable replication in CeresDB, select one instance to act as the ``Leader`` which all other instances will follow.  Then, configure the followers via the following env variables:

* ``CERESDB_LEADER`` -- The host and port of the leader to connect to, in the format ``<host>:<port>``
* ``CERESDB_FOLLOWER_AUTH`` -- The account credentials the follower should use to talk to the leader, in the format ``<username>:<password>``

This will disable write actions on the follower databases but will keep them in sync with the leader.

.. note:: As of version ``1.1.0`` replication should only be enabled for databases which are expected to hold a small amount of data
