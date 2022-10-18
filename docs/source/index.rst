***********************************
Welcome to CeresDB's documentation!
***********************************

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   access.rst
   authentication.rst
   configuring.rst
   querying.rst
   running.rst
   schema.rst
   replication.rst

About
=====

CeresDB is a database system designed to allow for the storage and retrieval of 
semi-structured data, i.e. data that conforms to a "top-level schema" where columns 
types are known, but those columns can in-turn contain dictionaries or lists.

Installation
============

CeresDB can be installed in one of two methods:

Local Installation
------------------

* Download an archive from the Releases_ page
* Extract the archive
* Place the ``ceresdb`` binary in your path
* Copy the extracted ``.ceresdb`` to your home directory
* Add ``export CERES_CONFIG_PATH=~/.ceres/config/config.json`` to your shell's rc file

See the :doc:`running` for information on spinning up your CeresDB instance

Docker Installation
-------------------

* Run ``docker pull jfcarter2358/ceresdb:<your desired version tag>``

See the :doc:`running` for information on spinning up your CeresDB instance

Contribute
==========

- Issue Tracker: github.com/jfcarter2358/ceresdb/issues
- Source Code: github.com/jfcarter2358/ceresdb

Support
=======

If you are having any issues please create an issue on GitHub_ or send an email to jfcarter2358@gmail.com

.. _GitHub: https://github.com/jfcarter2358/ceresdb
.. _Releases: https://github.com/jfcarter2358/ceresdb/releases

License
=======

The project is licensed under the MIT license.
