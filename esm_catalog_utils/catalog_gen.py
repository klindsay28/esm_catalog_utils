import ast
import datetime
import glob
import json
import os.path
import pprint
import re

import cftime
from dask import compute, delayed
import intake
from netCDF4 import Dataset
import pandas as pd
import yaml


def get_cases_metadata(case_metadata_paths, cases_snames):
    """
    return list of case metadata dicts from case_metadata_paths whose
    sname is in the list cases_snames
    """
    cases_metadata = []
    for case_metadata_path in case_metadata_paths:
        with open(case_metadata_path, mode="r") as fptr:
            cases_metadata_list = yaml.safe_load(fptr)
        for case_metadata in cases_metadata_list:
            if case_metadata["sname"] in cases_snames:
                cases_metadata.append(case_metadata)
    return cases_metadata


def cases_metadata_to_catalog(cases_metadata, path_pattern=None):
    """
    return a catalog from cases described by cases_metadata
    cases_metadata: list of case metadata dicts
    path_pattern: include only assest whose path matches path_pattern
    """

    df_list = []
    for ind, case_metadata in enumerate(cases_metadata):

        # generate esmcol files if they are not up to date
        if not esmcol_files_uptodate(case_metadata, path_pattern, debug=True):
            gen_esmcol_files(case_metadata, path_pattern)

        print(f"loading esmcol files for {case_metadata['sname']}")

        # read spec file, ensuring consistency across multiple spec files
        esmcol_spec_path = case_metadata["esmcol_spec_path"]
        spec_consistency_keys = ["attributes", "assets", "aggregation_control"]
        with open(esmcol_spec_path, mode="r") as fptr:
            esmcol_spec = json.load(fptr)
            if len(cases_metadata) > 1:
                if ind == 0:
                    path0 = esmcol_spec_path
                    spec0 = esmcol_spec
                else:
                    for key in spec_consistency_keys:
                        if esmcol_spec[key] != spec0[key]:
                            raise ValueError(
                                f"{key} mismatch in {esmcol_spec_path} and {path0}"
                            )

        # read catalog file
        # subset rows whose path matches path_pattern
        # append to df_list
        df = read_catalog(esmcol_spec["catalog_file"])
        if path_pattern is not None:
            df = df[df["path"].str.match(".*" + path_pattern + ".*")]
        df_list.append(df)

    esmcol_data = pd.concat(df_list, ignore_index=True)

    return intake.open_esm_datastore(esmcol_data, esmcol_spec)


def esmcol_files_uptodate(case_metadata, path_pattern, debug=False):
    """determine if esm collection files are up to date"""

    esmcol_spec_path = case_metadata["esmcol_spec_path"]

    # files are out of date if spec file doesn't exist
    if not os.path.isfile(esmcol_spec_path):
        if debug:
            print(f"spec file {esmcol_spec_path} doesn't exist")
        return False

    with open(esmcol_spec_path, mode="r") as fptr:
        esmcol_spec = json.load(fptr)

    catalog_file = esmcol_spec["catalog_file"]

    # files are out of date if data file doesn't exist
    if not os.path.isfile(catalog_file):
        if debug:
            print(f"data file {catalog_file} doesn't exist")
        return False

    # check if files in esmcol data file agree with files from glob
    esmcol_data = read_catalog(catalog_file)

    case = case_metadata["case"]
    for comp_metadata in case_metadata["components"]:
        scomp = comp_metadata["scomp"]
        esmcol_data_subset = esmcol_data[esmcol_data["scomp"] == scomp]
        col_paths = list(esmcol_data_subset["path"])
        # filter based on path_pattern, if specified
        if path_pattern is not None:
            col_paths = [path for path in col_paths if re.search(path_pattern, path)]
        paths = get_hist_paths(comp_metadata["histdir"], case, scomp, path_pattern)
        if sorted(col_paths) != paths:
            if debug:
                print(f"path mismatch for {scomp}")
                print(f"len(col_paths)={len(col_paths)}, len(paths)={len(paths)}")
            return False

    return True


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


def gen_esmcol_files(case_metadata, path_pattern):
    """
    generate files describing an esm collection for case described by case_metadata
    """

    print(f"generating esmcol files for {case_metadata['sname']}")

    esmcol_spec_path = case_metadata["esmcol_spec_path"]
    esmcol_data_path = esmcol_spec_path.rpartition(".")[0] + ".csv.gz"

    esmcol_spec = {
        "esmcat_version": "0.1.0",
        "id": "sample",
        "description": "This is a very basic sample ESM collection.",
        "catalog_file": esmcol_data_path,
        "attributes": [
            {"column_name": "case"},
            {"column_name": "scomp"},  # specific component name, used in filenames
            {"column_name": "path"},  # path for asset/file
            {"column_name": "stream"},  # name of stream that this asset/file is in
            {
                "column_name": "datestring"
            },  # datestring portion of filename, used for sorting
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
                "stream",
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

    with open(esmcol_spec_path, mode="w") as fptr:
        json.dump(esmcol_spec, fptr, indent=2)

    esmcol_data_rows = []

    case = case_metadata["case"]

    for comp_metadata in case_metadata["components"]:
        pprint.pprint(comp_metadata)
        scomp = comp_metadata["scomp"]

        paths = get_hist_paths(comp_metadata["histdir"], case, scomp, path_pattern)
        for path in paths:
            row = delayed(gen_esmcol_row)(path, case, scomp)
            esmcol_data_rows.append(row)

    esmcol_data_rows = list(compute(*esmcol_data_rows))
    # remove rows associated with restart stream
    esmcol_data_rows = [row for row in esmcol_data_rows if row["stream"] != "r"]

    df = pd.DataFrame(esmcol_data_rows)
    df.to_csv(esmcol_data_path, index=False)


def get_hist_paths(histdir, case, scomp, path_pattern):
    """return sorted list of paths of history files"""
    paths = glob.glob(f"{histdir}/{case}.{scomp}*.nc")
    # ignore restart stream
    rest_pattern = f"{case}.{scomp}.r."
    paths = [
        path for path in paths if not os.path.basename(path).startswith(rest_pattern)
    ]
    # filter based on path_pattern, if specified
    if path_pattern is not None:
        paths = [path for path in paths if re.search(path_pattern, path)]
    return sorted(paths)


def gen_esmcol_row(path, case, scomp):
    """return a dict of entries in an esmcol row"""

    row = {"case": case, "scomp": scomp, "path": path}
    path_attrs = path_to_attrs(path, case, scomp)
    row["stream"] = path_attrs["stream"]
    row["datestring"] = path_attrs.get("datestring", None)
    # don't extract attrs from restart stream
    if row["stream"] != "r":
        row.update(extract_file_attrs(path))
    return row


def path_to_attrs(path, case, scomp):
    """return dict of path attributes, e.g. stream, varname, datestring"""

    basename = os.path.basename(path)

    # remove {case}.{scomp}. prefix
    prefix = case + "." + scomp + "."
    if not basename.startswith(prefix):
        raise ValueError(f"{basename} does not start with {prefix}")
    remainder = basename[len(prefix) :]

    # remove extension
    remainder, _, _ = remainder.rpartition(".")

    # look for beginning of datestring, using pattern "[_.][0-9][0-9]"
    match_obj = re.search("[_.][0-9][0-9]", remainder)

    # if there is no datestring, infer this is a time invariant file
    # assume that such files are not single variable files
    # and that remainder is the stream
    if match_obj is None:
        return {"stream": remainder}

    attr_dict = {"datestring": remainder[match_obj.start() + 1 :]}
    remainder = remainder[: match_obj.start()]

    # if the datestring is a date range, infer that this is a single variable file
    # and that the last "." separated portion of the remainder is a varname
    # if date ranges are one of YYYY[_-]YYYY, YYYYMM[_-]YYYYMM, YYYYMMDD[_-]YYYYMMDD,
    # then they match the pattern [0-9][0-9][0-9][0-9][_-][0-9][0-9][0-9][0-9],
    # while a non-range datestring will not match this pattern
    pattern = "[0-9][0-9][0-9][0-9][_-][0-9][0-9][0-9][0-9]"
    if re.search(pattern, attr_dict["datestring"]):
        attr_dict["stream"], _, attr_dict["varname"] = remainder.rpartition(".")
    else:
        attr_dict["stream"] = remainder

    return attr_dict


def extract_file_attrs(path):
    """
    return dict of particular attributes from netCDF file specified by path
    Attributes returned are list of time-varying variables (excluding
    time:bounds), date_start and date_end. The latter 2 are end-points of
    time:bounds, if available, and end-points of time otherwise.
    uses netCDF4 API instead of xarray API for improved performance
    """

    attr_dict = {}

    time = "time"
    tb_name = ""

    with Dataset(path) as fptr:
        if time in fptr.variables:
            tb_name = fptr.variables[time].__dict__.get("bounds", "")
        attr_dict["varname"] = [
            name
            for name, var in fptr.variables.items()
            if time in var.dimensions and name not in {time, tb_name}
        ]

        if time not in fptr.variables:
            attr_dict["date_start"] = None
            attr_dict["date_end"] = None
            return attr_dict

        units = fptr.variables[time].units
        calendar = fptr.variables[time].calendar.lower()
        if tb_name:
            date_start = fptr.variables[tb_name][0, 0]
            date_end = fptr.variables[tb_name][-1, -1]
        else:
            date_start = fptr.variables[time][0]
            date_end = fptr.variables[time][-1]

    # convert model time values into date objects
    cftime_obj = cftime.num2date(date_start, units=units, calendar=calendar)
    attr_dict["date_start"] = datetime.date(
        cftime_obj.year, cftime_obj.month, cftime_obj.day
    )
    cftime_obj = cftime.num2date(date_end, units=units, calendar=calendar)
    attr_dict["date_end"] = datetime.date(
        cftime_obj.year, cftime_obj.month, cftime_obj.day
    )

    return attr_dict
