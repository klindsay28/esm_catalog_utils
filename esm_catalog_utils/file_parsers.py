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

    if "frequency" not in attr_dict:
        attr_dict["frequency"] = cesm_infer_freq(
            attr_dict["date_start"], attr_dict["date_end"], tb_name != "", tlen
        )

    return attr_dict


def cesm_infer_freq(date_start, date_end, time_bounds, tlen):
    """
    infer temporal frequency of time axis
    return None if frequency cannot be determined
    date_start and date_end are datetime objects representing a time interval
    time_bounds is a logical stating if date_start and date_end are from
        bounds or from a time variable
    tlen is the number of time levels spanned by date_start and date_end
    """

    # give up if there is 1 time level and no time bounds variable
    if not time_bounds and tlen == 1:
        return None

    if time_bounds:
        dt_avg = (date_end - date_start) / tlen
    else:
        dt_avg = (date_end - date_start) / (tlen - 1)

    if dt_avg == datetime.timedelta(hours=1):
        return "nhour_1"

    if dt_avg == datetime.timedelta(hours=3):
        return "nhour_3"

    if dt_avg == datetime.timedelta(hours=6):
        return "nhour_6"

    if dt_avg == datetime.timedelta(days=1):
        return "nday_1"

    if dt_avg == datetime.timedelta(days=5):
        return "nday_5"

    if dt_avg >= datetime.timedelta(days=28) and dt_avg <= datetime.timedelta(days=31):
        return "nmonth_1"

    if dt_avg >= datetime.timedelta(days=365) and dt_avg <= datetime.timedelta(
        days=366
    ):
        return "nyear_1"

    return None
