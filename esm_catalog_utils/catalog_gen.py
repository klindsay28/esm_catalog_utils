import datetime
import os
import os.path
from os import PathLike
from typing import Any, Callable, Dict, List, Optional, Union

import intake_esm
import pandas as pd
from dask import compute, delayed
from intake_esm import esm_datastore
from packaging import version

from .file_parsers import parse_file_cesm
from .path_parsers import parse_path_cesm


def case_metadata_to_esm_datastore(
    case_metadata: Dict[str, Any],
    exclude_dirs: List[str] = ["rest"],
    path_parser: Callable[
        [Union[str, PathLike], str], Dict[str, str]
    ] = parse_path_cesm,
    file_parser: Callable[[Union[str, PathLike]], Dict[str, Any]] = parse_file_cesm,
    esm_datastore_in: Optional[esm_datastore] = None,
    use_dask: bool = False,
) -> esm_datastore:
    """
    Generate `esm_datastore
    <https://intake-esm.readthedocs.io/en/stable/reference/api.html>`_
    object for files specified by case metadata.

    Parameters
    ----------
    case_metadata : dict
        Dict of metadata for case. It should have the following keys
        and corresponding values:

        - "case": name of case
        - "output_dirs": list of directories containing output from case
    exclude_dirs : list of str, optional
        Files in directories listed in `exclude_dirs` are disregarded.
    path_parser : callable, optional
        Function that returns a dict of attributes derived from pathnames
        of files in `output_dirs`. These attributes are included in
        columns of the DataFrame of the returned esm_datastore.
    file_parser : callable, optional
        Function that returns a dict of attributes derived from the
        contents of files in output_dirs. These attributes are included in
        columns of the DataFrame of the returned esm_datastore.
    esm_datastore_in : esm_datastore, optional
        If provided, then return an esm_datastore object with entries
        appended to `esm_datastore_in`. The paths determined from
        `case_metadata` are checked for existence in `esm_datastore_in`'s
        DataFrame df. If the path is present in df and the file's size
        differs from its size in `esm_datastore_in`, then the entry for
        that path is recreated. A `ValueError` is raised if df does not
        have an size column.
    use_dask : bool, optional
        If True, the parsing of file contents is performed in parallel
        using ``dask.delayed``. Default is False.

    Returns
    -------
    esm_datastore
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

    # create list of new rows for catalog

    esmcol_data_rows = []
    case: str = case_metadata["case"]
    paths = get_nc_paths(case_metadata["output_dirs"], case, exclude_dirs)
    if use_dask:
        for path in paths:
            path_in_size = paths_in_sizes.get(path, -1)
            row = delayed(gen_esmcol_row)(
                column_names, path, case, path_parser, file_parser, path_in_size
            )
            esmcol_data_rows.append(row)
        esmcol_data_rows = list(compute(*esmcol_data_rows))
    else:
        for path in paths:
            path_in_size = paths_in_sizes.get(path, -1)
            row = gen_esmcol_row(
                column_names, path, case, path_parser, file_parser, path_in_size
            )
            esmcol_data_rows.append(row)

    if esm_datastore_in is not None:
        # drop empty rows (these occur for up to date rows in esm_datastore_in)
        esmcol_data_rows = [row for row in esmcol_data_rows if row is not None]
        esmcol_data = pd.concat([esm_datastore_in.df, pd.DataFrame(esmcol_data_rows)])
    else:
        esmcol_data = pd.DataFrame(esmcol_data_rows)

    if version.Version(intake_esm.__version__) < version.Version("2022.9.18"):
        return esm_datastore(esmcol_data, esmcol_spec)
    else:
        return esm_datastore({"df": esmcol_data, "esmcat": esmcol_spec})


def get_nc_paths(
    dir_list: List[Union[str, PathLike]], case: str, exclude_dirs: List[str]
) -> List[str]:
    """
    Get paths of netCDF output files in directories and their subdirectories.

    Parameters
    ----------
    column_names : list of str or path-like
        Directories to be searched.
    case : str
        Name of case that generated files being searched for.
        Only files whose names start with case are returned.
    exclude_dirs : list of str
        Directories to exclude from the search.

    Returns
    -------
    list of str
        List of files that were found.
    """

    paths = []
    for dir in dir_list:
        for root, dirs, files in os.walk(str(dir).rstrip(os.sep)):
            if os.path.basename(root) in exclude_dirs:
                continue
            for file in files:
                if file.startswith(case) and file.endswith(".nc"):
                    paths.append(os.path.join(root, file))
    paths.sort()
    return paths


def gen_esmcol_row(
    column_names: List[str],
    path: Union[str, PathLike],
    case: str,
    path_parser: Callable[[Union[str, PathLike], str], Dict[str, str]],
    file_parser: Callable[[Union[str, PathLike]], Dict[str, Any]],
    path_in_size: int,
) -> Union[Dict[str, Any], None]:
    """
    Generate esmcol row from a file.

    Parameters
    ----------
    column_names : list of str
        Names of columns in esmcol, and keys in returned dict.
    path : str or path-like
        Path of file that esmcol row is being generated from.
    case : str
        Name of case that generated `path`.
    path_parser : callable
        Function to separate a file path into components.
    file_parser : callable
        Function to extract specific quantities/metadata from a file.
    path_in_size : int
        Cached value of size of file, or -1. If the same of the file
        `path` is equal to this value, then return None.

    Returns
    -------
    dict
        Dictionary of esmcol row entries, for each column in `column_names`.
    """

    # return None if path_in_size is equal to size of file specified by path,
    # i.e., row in esm_datastore_in in case_metadata_to_esm_datastore is up to date
    size = os.stat(path).st_size
    if path_in_size == size:
        return None

    path_attrs = path_parser(path, case)
    file_attrs = file_parser(path)
    row: Dict[str, Any] = {}
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


def date_parser(value: str) -> Union[datetime.date, None]:
    """
    Convert date string to date object.

    Useful as a converter in `pandas.read_csv`.

    Parameters
    ----------
    value : str
        Date string

    Returns
    -------
    datetime.date or None
        Date object corresponding to `value`, if `value` != "".
        None if `value` == "".
    """
    if value == "":
        return None
    else:
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
