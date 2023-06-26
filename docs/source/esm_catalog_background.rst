==================================
ESM Catalog Background Information
==================================

A simplified view of ESM catalogs is that they consist of the paths of
data files, metadata about these files, e.g., names of data variables in
the files and date ranges covered, and metadata about how the data files
can be aggregated together.

More generally, data files can reside in the cloud, in which case paths
are generalized to `URIs
<https://en.wikipedia.org/wiki/Uniform_Resource_Identifier>`_, and data
files can be in a format where their content is spread across multiple
files, e.g., `zarr <https://zarr.readthedocs.io/>`_.
Data files are referred to as assets, to recognize these generalizations.

Metadata about the assets referred to by an ESM catalog (paths or URIs,
data variable names, date ranges, etc.) is represented in memory in a
`pandas <https://pandas.pydata.org/>`_ `DataFrame
<https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_,
and on disk in a comma-separated values (CSV) file.

The primary data structure in `intake-esm` to support ESM catalogs is
the `esm_datatstore
<https://intake-esm.readthedocs.io/en/stable/reference/api.html>`_ class,
which consists of an `ESM Catalog Specification
<https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec.html>`_
and related functions.
The ESM Catalog Specification, sometimes referred to as an ESM Collection
Specification (esmcol_spec), consists of a dictionary of asset metadata
that is available, i.e., columns in the above-mentioned CSV file, metadata
about how the assets can be aggregated together, and some other metadata,
such as a description of the catalog.
The metadata regarding aggregation is stored in an `aggregation control object
<https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec.html#aggregation-control-object>`_.