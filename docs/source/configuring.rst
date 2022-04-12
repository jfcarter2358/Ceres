*******************
Configuring CeresDB
*******************

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Via Configuration File
======================

CeresDB can be configured either by providing a configuration JSON file of the 
following format:

.. code-block:: json

   {
      "log-level": "info",
      "home-dir": "~/.ceresdb",
      "data-dir": "~/.ceresdb/data",
      "index-dir": "~/.ceresdb/indices",
      "storage-line-limit": 16384,
      "port": 7437
   }

and then setting the environment variable ``CERESDB_CONFIG_PATH`` to point to said JSON 
file

Via Environment Variables
=========================

You can use environment variables to configure your Ceres instance with the following 
variables:

* ``CERESDB_LOG_LEVEL``
* ``CERESDB_HOME_DIR``
* ``CERESDB_DATA_DIR``
* ``CERESDB_INDEX_DIR``
* ``CERESDB_STORAGE_LINE_LIMIT``
* ``CERESDB_PORT``
* ``CERESDB_DEFAULT_ADMIN_PASSWORD``
