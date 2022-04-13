*************
Schema Format
*************

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Schemas take the form of a dictionary with each key being a field in the corresponding 
records that will be held in the collection and each value being the datatype of said 
field. Allowed datatypes are:

* ``STRING``
* ``INT``
* ``FLOAT``
* ``BOOL``
* ``LIST`` (not searchable by filters or able to be ordered)
* ``DICT`` (not searchable by filters or able to be ordered)
* ``ANY`` (not searchable by filters or able to be ordered)

An example schema is shown below:

.. code-block:: json

   {
      "a": "STRING",
      "b": "INT",
      "c": "FLOAT",
      "d": "BOOL",
      "e": "LIST",
      "f": "DICT",
      "g": "ANY"
   }
