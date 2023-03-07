***************************
Querying a CeresDB Instance
***************************

.. toctree::
   :maxdepth: 2
   :caption: Contents:

CeresDB uses the Antler Query Language (AQL) to interact with the data contained within 
the database. This language is made up of 9 main actions that can act on 5 different 
resources:

Collection
==========

Collections act as logical groupings of data with the same schema within a database. 
They are composed of multiple records.

.. note:: Details on Schema dictionaries can be found in the :doc:`schema` section

Delete
------

Deletes a collection from a database

.. code-block::

   DELETE COLLECTION <name of database>.<name of collection>

Get
---

Returns the collections contained in a database

.. code-block::

   GET COLLECTION <name of database>

Post
----

Creates a new collection

.. code-block::

   POST COLLECTION <name of database>.<name of collection> <dict of schema>

Put
---

Update the a collection's schema

.. code-block::

   POST COLLECTION <name of database>.<name of collection> <dict of schema>

Database
========

Databases act as the highest-level grouping of data which can contain multiple 
collections.

Delete
------

Deletes a database

.. code-block::

   DELETE DATABASE <name of database>

Get
---

Returns the databases contained in the CeresDB instance

.. code-block::

   GET DATABASE

Post
----

Creates a new database

.. code-block::

   POST DATABASE <name of database>

Permit
======

Permits control access to databases and are made up of records containing usernames 
within the instance and their roles.

.. note:: Details on access roles can be found in the :doc:`access` section

Delete
------

Deletes a permit

.. code-block::

   DELETE PERMIT <name of database> <id or list of ids of permit to delete or use '-' to delete ids from piped input>

Get
---

Returns the permits contained in a CeresDB database

.. code-block::

   GET PERMIT <name of database> <fields to include in output or use '*' to include all>

Post
----

.. note:: To use data piped into the post command, omit the dictionary at the end of the command
   
Creates a new permit

.. code-block::

   POST PERMIT <name of database> <dict of permit with format {"username":"<username to add>","role":"<access role to add>"}>

Put
---

.. note:: To use data piped into the put command, omit the dictionary at the end of the command
   
Overwrites a permit with new data

.. code-block::

   PUT PERMIT <name of database> <id or list of ids to overwrite> <dict or list of dicts of data to update to>

Record
======

Records are the items of data inserted/retrieved from the collections within a database.

Delete
------

Deletes a record

.. code-block::

   DELETE RECORD <name of database>.<name of collection> <id or list of ids of permit to delete or use '-' to delete ids from piped input>

Get
---

Returns the records within a specific database and collection

.. code-block::

   GET RECORD <name of database>.<name of collection> <fields to include in output or use '*' to include all>

Post
----

.. note:: To use data piped into the post command, omit the dictionary at the end of the command
   
Creates a new record

.. code-block::

   POST RECORD <name of database>.<name of collection> <dict or list of dicts of data to insert>

Patch
-----

.. note:: To use data piped into the patch command, omit the dictionary at the end of the command
   
Updates a field in multiple records

.. code-block::

   PATCH RECORD <name of database>.<name of collection> <id or list of ids to update> <dict of fields to update and their new values>

Put
---

.. note:: To use data piped into the put command, omit the dictionary at the end of the command
   
Overwrites a record with new data

.. code-block::

   PUT RECORD <name of database>.<name of collection> <id or list of ids to overwrite> <dict or list of dicts of data to update to>

.. _querying:user:

User
====

Users control who can access the database and what their instance-level permissions are.

.. note:: Details on access roles can be found in the :doc:`access` section

Delete
------

Deletes a user

.. code-block::

   DELETE USER <id or list of ids of permit to delete or use '-' to delete ids from piped input>

Get
---

Returns the users contained in a CeresDB instance

.. code-block::

   GET USER <fields to include in output or use '*' to include all>

Post
----

.. note:: To use data piped into the post command, omit the dictionary at the end of the command

Creates a new user

.. code-block::

   POST USER <dict of permit with format {"username":"<username to add>","role":"<access role to add>","password":"<password for the user to authenticate with>"}>

Put
---

.. note:: To use data piped into the put command, omit the dictionary at the end of the command

Overwrites a user with new data

.. code-block::

   PUT USER <id or list of ids to overwrite> <dict or list of dicts of data to update to>

Modifier Actions
================

While CeresDB uses ``DELETE``, ``GET``, ``PATCH``, ``POST``, and ``PUT`` to manipulate 
data, the results of these actions can then be piped into others to perform complex 
actions. The modifier actions that output can be piped into (in addition to piping the 
output into statements which take the ``-`` as an argument described above) are:

Count
-----

Returns the number of items returned from the input query in the format 
``{"count":"<number of items>"}``

.. code-block::

   <Other query> | COUNT


Filter
------

.. note:: Filter can only be used on GET queries for records, users, and permits

Allows you to filter out the results of a ``GET`` query using logical expressions made 
up of ``<field name> <comparison operator> <value>`` joined together via logical 
operators

.. code-block::

   <Other query> | FILTER <field name> <comparison operator> <value> <logical operator> ...

JQ
--

Allows you to process data piped into the command using a JQ string.

.. code-block::
   
   <Other query> | JQ '<your JQ string here>'

.. note:: The JQ string *must* be wrapped in single quotes

.. note:: The JQ command can be used to modify data entirely on the server by piping a ``GET`` command into a ``JQ`` command and then piping that into a ``PUT`` command

.. note:: Do note use the JQ operator to select data (i.e. `JQ '.[].hello'` on data structured like `{"hello":"..."}`) as this will produce an invalid result (all data should be returned as a list of dictionaries, not single type values)


Comparison Operators
^^^^^^^^^^^^^^^^^^^^

* ``>`` Greater than
* ``>=`` Greater than or equal to
* ``=`` Equal to
* ``<`` Less than
* ``<=`` Less than or equal to
* ``!=`` Not equal to

Logical Operators
^^^^^^^^^^^^^^^^^

* ``AND`` And
* ``OR`` Or
* ``NOT`` Not
* ``XOR`` Exclusive or

Limit
-----

Allows you to reduce the number of results to a specified maximum

.. code-block::

   <Other query> | LIMIT <maximum desired number of items>


Orderasc
--------

Orders results in ascending order by a specified key

.. code-block::

   <Other query> | ORDERASC <key to order by>


Orderdsc
--------

Orders results in descending order by a specified key

.. code-block::

   <Other query> | ORDERDSC <key to order by>

