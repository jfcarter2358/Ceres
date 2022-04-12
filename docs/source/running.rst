***************
Running CeresDB
***************

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Running Locally
===============

To run CeresDB locally, ensure that the ``ceresdb`` binary is in your path and configure 
your instance using the information in :doc:`configuring`. 
Afterwards, just run the ``ceresdb`` binary and your instance will start.

Running in Docker
=================

To run CeresDB via Docker, pull down the CeresDB image with

.. code-block::

   docker pull jfcarter2358/ceresdb:<desired tag>

Then run it with

.. code-block::
   
   docker run -p 7437:7437 jfcarter2358/ceresdb:<desired tag>

If you want the database to persist after the container is killed, use environment 
variables to configure the CeresDB data and index directories as detailed in 
in :doc:`configuring` to direct them to volumes mounted into the 
container.
