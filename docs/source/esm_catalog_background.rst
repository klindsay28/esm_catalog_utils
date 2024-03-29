==================================
ESM Catalog Background Information
==================================

A simplified view of ESM catalogs is that they consist of the paths of
ESM output files, metadata about these files, e.g., names of data variables
in the files and date ranges covered, and metadata about how the data files
can be aggregated together.

More generally, data files can reside in the cloud, in which case `URIs
<https://en.wikipedia.org/wiki/Uniform_Resource_Identifier>`_ are used
instead of paths, and data files can be in a format where their content is
spread across multiple files, e.g., :std:doc:`zarr <zarr:index>`.
In the following, ESM output is referred to as assets, to recognize these
generalizations.

Metadata about the assets referred to by an ESM catalog (paths or URIs,
data variable names, date ranges, etc.) is stored in memory in a
:py:mod:`pandas` :py:class:`~pandas.DataFrame` object, and on disk in a
comma-separated values (CSV) file.

The primary data structure in :std:doc:`intake-esm <intake-esm:index>`
to support ESM catalogs is the :std:doc:`esm_datastore
<intake-esm:reference/api>` class.
Loosely speaking, this class consists of an
:std:doc:`intake-esm:reference/esm-catalog-spec` and functions that operate
on class objects.
The :std:doc:`intake-esm:reference/esm-catalog-spec` consists of a
dictionary of asset metadata that is available, i.e., columns in the
above-mentioned CSV file, metadata about how the assets can be aggregated
together, and some other metadata, such as a description of the catalog.
The metadata regarding aggregation is stored in an `aggregation control object
<https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec.html#aggregation-control-object>`_.