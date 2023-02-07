"""functions to extract catalog entries from files"""

import datetime

import cftime
from netCDF4 import Dataset


def parse_file_cesm(path):
    """
    return dict of particular attributes from netCDF file specified by path
    Attributes returned are list of time-varying variables (excluding
    time:bounds), output frequency, date_start and date_end. The last 2 are
    end-points of time:bounds, if available, and end-points of time otherwise.
    uses netCDF4 API instead of xarray API for improved performance
    """

    # TODO: figure out how/if to handle ww3 files, that have no time variable
    # TODO: generate frequency when file doesn't have time_period_freq attribute

    attr_dict = {}

    time = "time"
    tb_name = ""

    with Dataset(path, mode="r") as fptr:
        fptr.set_auto_mask(False)
        if time in fptr.variables and "bounds" in fptr.variables[time].ncattrs():
            tb_name = fptr.variables[time].bounds
            if tb_name not in fptr.variables:
                raise RuntimeError("specified bounds variable not found in %s" % path)
        attr_dict["varname"] = [
            name
            for name, var in fptr.variables.items()
            if time in var.dimensions and name not in {time, tb_name}
        ]

        if "time_period_freq" in fptr.ncattrs():
            attr_dict["frequency"] = fptr.time_period_freq

        if time not in fptr.variables:
            attr_dict["date_start"] = None
            attr_dict["date_end"] = None
            return attr_dict

        units = fptr.variables[time].units
        calendar = fptr.variables[time].calendar.lower()
        tlen = len(fptr.dimensions["time"])
        if tb_name:
            date_start = fptr.variables[tb_name][0, 0]
            date_end = fptr.variables[tb_name][tlen - 1, 1]
        else:
            date_start = fptr.variables[time][0]
            date_end = fptr.variables[time][tlen - 1]

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
