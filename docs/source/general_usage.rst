=============
General Usage
=============

Creating a Catalog
------------------

Catalogs, i.e. :std:doc:`esm_datastore <intake-esm:reference/api>` objects,
are created in :mod:`esm_catalog_utils` from a casename and a list of
directories containing model output.
The casename and list of directories are stored in a dictionary with
keys ``case`` and ``output_dirs`` respectively.
We refer to this dictionary as ``case_metadata``.
The function :func:`~esm_catalog_utils.case_metadata_to_esm_datastore`
takes a *case_metadata* argument and returns a :std:doc:`esm_datastore
<intake-esm:reference/api>` object for the output files in ``output_dirs``
and its subdirectories.
Additional arguments are described in its :func:`API documentation
<esm_catalog_utils.case_metadata_to_esm_datastore>`.

:mod:`esm_catalog_utils` also provides helper functions that generates
the ``case_metadata`` dictionary in particular use cases, calls :func:`~esm_catalog_utils.case_metadata_to_esm_datastore`, and
returns the result.

:func:`~esm_catalog_utils.directory_to_esm_datastore` is a helper function
for the use case of having model output in a single top-level directory
and its subdirectories.
The *dir* argument of :func:`~esm_catalog_utils.directory_to_esm_datastore`
is the top-level directory where the model output is located.
The casename can be either passed as the *case* argument to 
:func:`~esm_catalog_utils.directory_to_esm_datastore`
or inferred from the basename of *dir*.

:func:`~esm_catalog_utils.caseroot_to_esm_datastore` is a helper function
that takes a *caseroot* argument.
It determines the ``case_metadata``, the casename and location of the model
output, from the xml files in *caseroot*.

Additional arguments to these helper functions are passed through to
:func:`~esm_catalog_utils.case_metadata_to_esm_datastore`.
Example usage of these helper funcions is provided in the
:ref:`notebooks`.

Parallelization
~~~~~~~~~~~~~~~

Extracting the metadata from model output files, such as the data variable
names and date ranges, involves opening the files and examining the file's
metadata.
For long runs, there can tens of thousands of native model history files.
Opening all of these files and examining their metadata can take a
considerable amount of time.
In order to speed up this process,
:func:`~esm_catalog_utils.case_metadata_to_esm_datastore` can use
:std:doc:`dask:index` to accelerate this embarrassingly parallel task.
If the *use_dask* argument to
:func:`~esm_catalog_utils.case_metadata_to_esm_datastore` is ``True``, then
it will wrap the file open and query operations inside
:std:doc:`dask:index` :py:class:`~dask.delayed.Delayed` objects and execute
them in parallel.

This should only be done if
:func:`~esm_catalog_utils.case_metadata_to_esm_datastore` is called after
instantiating a :std:doc:`dask.distributed:index`
:py:class:`~distributed.Client`, as otherwise an error may be raised.
The default value for *use_dask* is ``False``.

The *use_dask* argument can also be passed to the helper functions
:func:`~esm_catalog_utils.directory_to_esm_datastore` and
:func:`~esm_catalog_utils.caseroot_to_esm_datastore`, and it will be passed
through to :func:`~esm_catalog_utils.case_metadata_to_esm_datastore`.

Writing and Reading a Catalog
-----------------------------

:std:doc:`esm_datastore <intake-esm:reference/api>` objects can be written
to disk using the object's :func:`serialize` method, which is documented in
the intake-esm :std:doc:`intake-esm:reference/api`.
The resulting files can be read using :func:`intake.open_esm_datastore`.
Example usage of these methods and functions is provided in the
:ref:`notebooks`.

Updating a Catalog
------------------

Even with the parallel speed-up provided by *use_dask*, generating a
catalog for a long run takes a non-trivial amount of time.
A use case for analysis of ESM output that regularly occurs, particularly
during a development cycle, is to analyze a run, extend the run, and
analyze the extended run.
:func:`~esm_catalog_utils.case_metadata_to_esm_datastore` has an argument
named *esm_datastore_in* to accelerate this use case.
If this argument is passed,
:func:`~esm_catalog_utils.case_metadata_to_esm_datastore` will return an
:py:class:`esm_datastore` object with entries appended to
*esm_datastore_in*.
The paths determined from the *case_metadata* argument to
:func:`~esm_catalog_utils.case_metadata_to_esm_datastore` are checked for
existence in *esm_datastore_in*'s DataFrame ``df``.
If the path is present in ``df`` and the file's size differs from its size
in *esm_datastore_in*, then the entry for that path is recreated.
If the file's size is the same as its size in *esm_datastore_in*,
then that file's catalog entry is propagated without reopening the file
and querying its metadata.
Because checking a file's size is much faster than this metadata query,
this option provides a considerable speed-up in this use case.

The *esm_datastore_in* argument can also be passed to the helper functions
:func:`~esm_catalog_utils.directory_to_esm_datastore` and
:func:`~esm_catalog_utils.caseroot_to_esm_datastore`, and it will be passed
through to :func:`~esm_catalog_utils.case_metadata_to_esm_datastore`.

Example usage of the *esm_datastore_in* is provided in the
:ref:`notebooks`.

Catalog Issues Specific to History Files
----------------------------------------
In some model analysis use cases, the model output being analyzed has been
post-processed into files that have a single data variable per file.
In contrast, native model history file output, the files written directly
by ESMs, typically has multiple data variables per file.
In this use case, the `varname` column of the CSV file component of the
ESM catalog is a list.
Additional steps are necessary to properly parse such files when calling
:func:`intake.open_esm_datastore`.
As described in the :std:doc:`intake-esm documentation
<intake-esm:how-to/use-catalogs-with-assets-containing-multiple-variables>`,
one approach to handle this use case is to pass the value
``{"converters": {"varname": ast.literal_eval}}`` to the *read_csv_kwargs*
argument of :func:`intake.open_esm_datastore` when
reading the catalog.
This is demonstrated in the :doc:`history file example notebook
<notebooks/ex1_caseroot_hist>`.