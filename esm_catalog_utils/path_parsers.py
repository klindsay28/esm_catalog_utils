"""functions to decompose paths into dicts of path components"""

import os.path
import re


def cesm_scomp_to_component(scomp):
    """
    Return component value for provided scomp.
    Return scomp itself if it is not found in comp_dict.
    """

    comp_dict = {
        "cpl": "cpl",
        "cam": "atm",
        "clm2": "lnd",
        "clm": "lnd",
        "cice": "ice",
        "mom6": "ocn",
        "pop": "ocn",
        "mosart": "rof",
        "rtm": "rof",
        "cism": "glc",
        "ww3": "wav",
    }
    return comp_dict.get(scomp, scomp)


def parse_path_cesm(path, case):
    """
    Return dict of path components from a CESM output file path.

    The returned dict has key-value pairs for the following keys, if applicable:
    scomp: specific model component name immediately after case, e.g., cam, clm2, mom6
    component: generic component name, derived from scomp
    stream: name of output steam, e.g., h, h0, h.nday1
    varname: name of variable, for timeseries files
    datestring: string represent date, if present

    Assumes that timeseries files have a datestring with a date range.
    """

    # TODO: handle multi-instance, like case.pop_0001.h.0001-01.nc

    # strip dirname and extension from path
    stem = os.path.splitext(os.path.basename(path))[0]

    # remove {case}. prefix
    prefix = case + "."
    if not stem.startswith(prefix):
        raise ValueError(f"{stem} does not start with {prefix}")
    remainder = stem[len(prefix) :]

    attr_dict = {}

    # extract scomp
    attr_dict["scomp"], _, remainder = remainder.partition(".")

    attr_dict["component"] = cesm_scomp_to_component(attr_dict["scomp"])

    # look for beginning of datestring, using pattern "[_.][0-9][0-9]"
    match_obj = re.search("[_.][0-9][0-9]", remainder)

    if match_obj is not None:
        attr_dict["datestring"] = remainder[match_obj.start() + 1 :]
        remainder = remainder[: match_obj.start()]

        # If the datestring is a date range, infer that this is a single variable file
        # and that the last "." separated portion of the remainder is a varname. If
        # date ranges are one of YYYY[_-]YYYY, YYYYMM[_-]YYYYMM, YYYYMMDD[_-]YYYYMMDD,
        # then they match the pattern [0-9][0-9][0-9][0-9][_-][0-9][0-9][0-9][0-9],
        # while a non-range datestring will not match this pattern.
        pattern = "[0-9][0-9][0-9][0-9][_-][0-9][0-9][0-9][0-9]"
        if re.search(pattern, attr_dict["datestring"]):
            attr_dict["stream"], _, attr_dict["varname"] = remainder.rpartition(".")
        else:
            # path is a history file, with no varname
            attr_dict["stream"] = remainder
    else:
        # path is a time-invariant file
        attr_dict["stream"] = remainder

    return attr_dict
