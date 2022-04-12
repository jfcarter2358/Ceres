***************************
Authentication with CeresDB
***************************

.. toctree::
   :maxdepth: 2
   :caption: Contents:

CeresDB authenticates users with a username and password which are stored (salted and 
hashed in the case of the password) within the CeresDB instance. By default, CeresDB 
creates an admin user on startup with the following credentials:

+----------+---------+
| Username | ceresdb |
+----------+---------+
| Password | ceresdb |
+----------+---------+

The admin password can be controlled by setting the ``CERESDB_DEFAULT_ADMIN_PASSWORD`` 
environment variable.

To see how to manage the users within a CeresDB instance, see the :ref:`querying:user` 
section of :doc:`querying`
