=============
General Usage
=============

Creating a Catalog
------------------

Parallelization
---------------

Extracting the metadata from model output files, such as the data variable
names and date ranges, involves opening the files and examining the file's
metadata.
For long runs, there can tens of thousands of native model history files.
Opening all of these files and examining their metadata can take a
considerable amount of time.
In order to speed up this process, `case_metadata_to_esm_datastore` can
use `dask <https://docs.dask.org/>`_ to accelerate this embarrassingly
parallel task.
If the `use_dask` argument to `case_metadata_to_esm_datastore` is `True`,
then it will wrap the file open and query operations inside dask `Delayed
<https://docs.dask.org/en/stable/delayed.html>`_ objects and execute them
in parallel.

This should only be done if `case_metadata_to_esm_datastore` is called
after instantiating a dask distributed `Client`.
The default value for `use_dask` is `False`.

The `use_dask` argument can also be passed to the helper functions
`directory_to_esm_datastore` and `caseroot_to_esm_datastore`, and it
will be passed through to `case_metadata_to_esm_datastore`.

Updating a Catalog
------------------

Even with the parallel speed-up provided by `use_dask`, generating a
catalog for a long run takes a non-trivial amount of time.
A use case for analysis of ESM output that regularly occurs, particularly
during a development cycle, is to analyze a run, extend the run, and
analyze the extended run.
`case_metadata_to_esm_datastore` has an argument named `esm_datastore_in`
to accelerate this use case.
If this argument, whose default value is `None`, is passed,
`case_metadata_to_esm_datastore` will return an `esm_datastore` object
with entries appended to `esm_datastore_in`.
The paths determined from `case_metadata` argument to
`case_metadata_to_esm_datastore` are checked for existence in
`esm_datastore_in`'s DataFrame `df`.
If the path is present in `df` and the file's size differs from its size
in `esm_datastore_in`, then the entry for that path is recreated.
If the file's size is the same as its size in `esm_datastore_in`,
then that file's catalog entry is propagated without reopening the file
and querying its metadata.
Because checking a file's size is much faster than this metadata query,
this option provides a considerable speed-up in this use case.

The `esm_datastore_in` argument can also be passed to the helper functions
`directory_to_esm_datastore` and `caseroot_to_esm_datastore`, and it
will be passed through to `case_metadata_to_esm_datastore`.

Usage of the `esm_datastore_in` is demonstrated in the example notebooks.

Catalog Issues Specific to History Files
----------------------------------------
In some model analysis use cases, such as analyzing output in the CMIP
archive, the model output has been post-processed into files that have a
single data variable per file.
In contrast, native model history file output, the files written directly
by ESMs, typically have multiple data variables per file.
In this situation, the `varname` column of the CSV component of the
ESM catalog is a list.
Such a list is stored in the CSV file as a quoted string of comma-separated
strings.
In order to properly parse this column when reading such a catalog,
it is necessary to pass the argument
``read_csv_kwargs={"converters": {"varname": ast.literal_eval}}``
to `intake.open_esm_datastore` when reading the catalog.
This is demonstrated in the history file example notebook.