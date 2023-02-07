import ast
import datetime
import os
import os.path

from dask import compute, delayed
from dask.distributed import get_client
import intake_esm
import pandas as pd

from .path_parsers import parse_path_cesm
from .file_parsers import parse_file_cesm


def case_metadata_to_esm_datastore(
    case_metadata,
    exclude_dirs=["rest"],
    path_parser=parse_path_cesm,
    file_parser=parse_file_cesm,
):
    """
    return esm_datastore object for case described by case_metadata
    """

    print(f"generating esm_datastore for {case_metadata['case']}")

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
            {
                "column_name": "datestring"
            },  # datestring portion of filename, used for sorting
            {"column_name": "frequency"},  # frequency of output
            {"column_name": "date_start"},  # date portion of initial time in file
            {"column_name": "date_end"},  # date portion of end time in file
            {"column_name": "varname"},
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

    # convert files in specified paths into rows in esmcol_data_rows

    esmcol_data_rows = []
    case = case_metadata["case"]
    paths = get_paths(case_metadata["output_dirs"], case, exclude_dirs)
    if client is not None:
        for path in paths:
            row = delayed(gen_esmcol_row)(
                column_names, path, case, path_parser, file_parser
            )
            esmcol_data_rows.append(row)
            esmcol_data_rows = list(compute(*esmcol_data_rows))
    else:
        for path in paths:
            row = gen_esmcol_row(column_names, path, case, path_parser, file_parser)
            esmcol_data_rows.append(row)

    # remove rows associated with restart stream
    esmcol_data_rows = [row for row in esmcol_data_rows if row["stream"] != "r"]

    return intake_esm.core.esm_datastore(
        {"esmcat": esmcol_spec, "df": pd.DataFrame(esmcol_data_rows)}
    )


def get_paths(dir_list, case, exclude_dirs):
    """return paths in dirs"""

    paths = []
    for dir in dir_list:
        for root, dirs, files in os.walk(dir.rstrip(os.sep)):
            if os.path.basename(root) in exclude_dirs:
                continue
            files.sort()
            for file in files:
                if file.startswith(case) and file.endswith(".nc"):
                    paths.append(os.path.join(root, file))

    return paths


def gen_esmcol_row(column_names, path, case, path_parser, file_parser):
    """return a dict of entries in an esmcol row"""

    path_attrs = path_parser(path, case)
    file_attrs = file_parser(path)
    row = {}
    for key in column_names:
        if key == "path":
            row[key] = path
        elif key == "case":
            row[key] = case
        elif key in path_attrs:
            row[key] = path_attrs[key]
        elif key in file_attrs:
            row[key] = file_attrs[key]
    return row


def date_parser(value):
    """parse date string to date object"""
    if value == "":
        return None
    else:
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()


def read_catalog(path):
    """read csv catalog"""
    return pd.read_csv(
        path,
        converters={
            "variable": ast.literal_eval,
            "date_start": date_parser,
            "date_end": date_parser,
        },
    )
