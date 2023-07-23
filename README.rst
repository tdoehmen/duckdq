======
DuckDQ
======


DuckDQ: Embedabble Data Quality Validation


Description
===========

DuckDQ is a python library that provides a fluent API for the definition and execution of:

* data quality checks for ML pipelines, built upon the estimator/transformer paradigm of scikit-learn.
* data quality checks on:
    * pandas dataframes
    * CSV files
    * parquet files
    * database tables from relational database systems

The design of DuckDQ is inspired by Deequ and combines it with the power of the embeddable analytical database management system DuckDB.
It excels on small-to medium-sized datasets.

Installation
===========
.. code-block::

   git clone https://github.com/tdoehmen/duckdq 
   cd duckdq 
   python setup.py install 

Citation
===========
.. code-block::

   @article{duckdq2021,
     author = {Doehmen, Till and Raasveldt, Mark and Muehleisen, Hannes and Schelter, Sebastian},
     journal={Challenges in Deploying and monitoring Machine Learning Systems Workshop, ICML 2021},
     title = {{DuckDQ: Data Quality Assertions for Machine Learning Pipelines}},
     year = {2021}
   }

`PDF <https://ssc.io/pdf/duckdq.pdf>`_

Acknowledgements
==================

`Deequ <https://github.com/awslabs/deequ>`_ 

`DuckDB <https://github.com/cwida/duckdb>`_ 

We adopted parts of the user-facing API and unit-tests from the `hooqu <https://github.com/mfcabrera/hooqu>`_ project under Apache License 2.0

