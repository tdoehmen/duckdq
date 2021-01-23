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

Acknowledgements
==================

Deequ https://github.com/awslabs/deequ

DuckDB https://github.com/cwida/duckdb

We adopted parts of the user-facing API and unit-tests from the hooqu project https://github.com/mfcabrera/hooqu under Apache License 2.0

