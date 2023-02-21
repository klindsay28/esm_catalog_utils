import datetime
import os
import os.path

from dask import compute, delayed
from dask.distributed import get_client
import intake_esm
from packaging import version
import pandas as pd

from .path_parsers import parse_path_cesm
from .file_parsers import parse_file_cesm


def case_metadata_to_esm_datastore(
    case_metadata,
    exclude_dirs=["rest"],
    path_parser=parse_path_cesm,
    file_parser=parse_file_cesm,
    esm_datastore_in=None,
):
    """
    return esm_datastore object for case described by case_metadata

    case_metadata is a dict with two expected keys, case and output_dirs.
    The value for case is a casename, and the value for output_dirs is a
    list of directories containing case output. The returned esm_datastore
    object is based on paths in these directories, and subdirectories.

    Files in directories listed in exclude_dirs are disregarded.

    path_parser is a function that returns a dict of attributes derived
    from pathnames of files in output_dirs. These attributes are included
    in columns of the DataFrame of the returned esm_datastore. An example of
    such an attribute is the pathname's datestring.

    file_parser is a function that returns a dict of attributes derived
    from the contents of files in output_dirs. These attributes are included
    in columns of the DataFrame of the returned esm_datastore. Examples of
    such attributes are date_start, date_end, and varname.

    If esm_datastore_in is provided, then return an esm_datastore object
    with entries appended to esm_datastore_in. The paths determined from
    case_metadata are checked for existence in esm_datastore_in's DataFrame
    df. If the path is present in df and the file's size differs from its
    size in esm_datastore_in, then the entry for that path is recreated. Raise
    an error if df does not have an size column.
    """

    verb = "generating" if esm_datastore_in is None else "appending"
    print(f"{verb} esm_datastore for {case_metadata['case']}")

    # If esm_datastore_in is provided then
    #   ensure that it has a size column
    #   create path:size dict for determining if rows are up to date
    #   use esmcol_spec from it
    if esm_datastore_in is not None:
        if "size" not in esm_datastore_in.df.columns:
            raise ValueError(
                "no size column in DataFrame from provided esm_datastore_in"
            )
        paths_in_sizes = esm_datastore_in.df.set_index("path")["size"].to_dict()
        if version.Version(intake_esm.__version__) < version.Version("2022.9.18"):
            esmcol_spec = esm_datastore_in.esmcol_data
        else:
            esmcol_spec = esm_datastore_in.esmcat.dict()
    else:
        paths_in_sizes = {}
        esmcol_spec = {
            "esmcat_version": "0.1.0",
            "id": "sample",
            "description": "This is a very basic sample ESM collection.",
            "attributes": [
                {"column_name": "case"},
                {"column_name": "scomp"},  # specific component name, used in filenames
                {"column_name": "component"},  # generic component name
                {"column_name": "path"},  # path for asset/file
                {"column_name": "stream"},  # name of stream that this asset/file is in
                {"column_name": "datestring"},  # datestring portion of filename
                {"column_name": "frequency"},  # frequency of output
                {"column_name": "date_start"},  # date portion of initial time in file
                {"column_name": "date_end"},  # date portion of end time in file
                {"column_name": "varname"},  # name(s) of variables in file
                {"column_name": "size"},  # size of file
            ],
            "assets": {"column_name": "path", "format": "netcdf"},
            "aggregation_control": {
                "variable_column_name": "varname",
                # columns whose entries must agree for rows to be aggregatable
                "groupby_attrs": [
                    "case",
                    "scomp",
                    "component",
                    "stream",
                    "frequency",
                ],
                "aggregations": [
                    {"type": "union", "attribute_name": "varname"},
                    {
                        "type": "join_existing",
                        "attribute_name": "datestring",
                        "options": {
                            "dim": "time",
                            "coords": "minimal",
                            "compat": "override",
                        },
                    },
                ],
            },
        }

    column_names = [attribute["column_name"] for attribute in esmcol_spec["attributes"]]

    # determine if a dask.distributed.Client has been instantiated
    try:
        client = get_client()
        # avoid threads because of
        # https://github.com/Unidata/netcdf4-python/issues/1192
        if max(client.nthreads().values()) > 1:
            raise RuntimeError(
                "netCDF4 is not thread-safe, "
                "use threads_per_worker=1 when instantiating dask.distributed.Client"
            )
    except ValueError:
        client = None

    # create list of new rows for catalog

    esmcol_data_rows = []
    case = case_metadata["case"]
    paths = get_paths(case_metadata["output_dirs"], case, exclude_dirs)
    if client is not None:
        for path in paths:
            row = delayed(gen_esmcol_row)(
                column_names, path, case, path_parser, file_parser, paths_in_sizes
            )
            esmcol_data_rows.append(row)
            esmcol_data_rows = list(compute(*esmcol_data_rows))
    else:
        for path in paths:
            row = gen_esmcol_row(
                column_names, path, case, path_parser, file_parser, paths_in_sizes
            )
            esmcol_data_rows.append(row)

    if esm_datastore_in is not None:
        # drop empty rows (these occur for up to date rows in esm_datastore_in)
        esmcol_data_rows = [row for row in esmcol_data_rows if row is not None]
        esmcol_data = pd.concat([esm_datastore_in.df, pd.DataFrame(esmcol_data_rows)])
    else:
        esmcol_data = pd.DataFrame(esmcol_data_rows)

    if version.Version(intake_esm.__version__) < version.Version("2022.9.18"):
        return intake_esm.core.esm_datastore(esmcol_data, esmcol_spec)
    else:
        return intake_esm.core.esm_datastore({"df": esmcol_data, "esmcat": esmcol_spec})


def get_paths(dir_list, case, exclude_dirs):
    """
    return paths in dirs
    Exclude files in directories listed in exclude_dirs.
    Only include paths ending with ".nc" and starting with case.
    """

    paths = []
    for dir in dir_list:
        for root, dirs, files in os.walk(dir.rstrip(os.sep)):
            if os.path.basename(root) in exclude_dirs:
                continue
            for file in files:
                if file.startswith(case) and file.endswith(".nc"):
                    paths.append(os.path.join(root, file))
    paths.sort()
    return paths


def gen_esmcol_row(column_names, path, case, path_parser, file_parser, paths_in_sizes):
    """return a dict of entries in an esmcol row"""

    # return None if paths_in_sizes[path] is equal to size of file specified by path,
    # i.e., row in esm_datastore_in in case_metadata_to_esm_datastore is up to date
    size = os.stat(path).st_size
    if paths_in_sizes.get(path, -1) == size:
        return None

    path_attrs = path_parser(path, case)
    file_attrs = file_parser(path)
    row = {}
    for key in column_names:
        if key == "path":
            row[key] = path
        elif key == "case":
            row[key] = case
        elif key == "size":
            row[key] = size
        elif key in path_attrs:
            row[key] = path_attrs[key]
        elif key in file_attrs:
            row[key] = file_attrs[key]
        else:
            raise ValueError(f"unknown column name {key} for {path}")
    return row


def date_parser(value):
    """parse date string to date object"""
    if value == "":
        return None
    else:
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
