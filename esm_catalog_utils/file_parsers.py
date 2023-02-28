"""functions to extract catalog entries from files"""

import datetime

import cftime
from netCDF4 import Dataset


def parse_file_cesm(path):
    """
    return dict of particular attributes from netCDF file specified by path
    Attributes returned are
    varname: list of time-varying variables (excluding time:bounds),
    frequency: output frequency,
    date_start, date_end: datetime.date objects for end-points of time:bounds,
        if available, and end-points of time otherwise.
    If there is no time variable, then all variables are included in varname.
    uses netCDF4 API instead of xarray API for improved performance
    """

    # TODO: figure out how/if to handle ww3 files, that have no time variable

    attr_dict = {}

    time = "time"
    tb_name = ""

    with Dataset(path, mode="r") as fptr:
        fptr.set_auto_mask(False)

        if time not in fptr.variables:
            attr_dict["date_start"] = datetime.date(1, 1, 1)
            attr_dict["date_end"] = datetime.date(1, 1, 1)
            attr_dict["varname"] = list(fptr.variables)
            attr_dict["frequency"] = "unknown"
            return attr_dict

        names_omit = [time]
        if "bounds" in fptr.variables[time].ncattrs():
            tb_name = fptr.variables[time].bounds
            if tb_name not in fptr.variables:
                raise RuntimeError("specified bounds variable not found in %s" % path)
            names_omit.append(tb_name)

        attr_dict["varname"] = [
            name
            for name, var in fptr.variables.items()
            if time in var.dimensions and name not in names_omit
        ]

        # if no time-varying variables exist, then include all non-coordinate variables
        if not attr_dict["varname"]:
            attr_dict["varname"] = [
                name for name in fptr.variables if name not in fptr.dimensions
            ]

        if "time_period_freq" in fptr.ncattrs():
            attr_dict["frequency"] = fptr.time_period_freq

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
            calendar,
            attr_dict["date_start"],
            attr_dict["date_end"],
            tb_name != "",
            tlen,
        )

    return attr_dict


def date_to_datetime(date):
    """convert datetime.date object to datetime.datetime object"""
    return datetime.datetime(date.year, date.month, date.day)


def cesm_infer_freq(calendar, date_start, date_end, time_bounds, tlen):
    """
    infer temporal frequency of time axis
    return the empty string if frequency cannot be determined
    date_start and date_end are datetime.date objects representing a time interval
    time_bounds is a logical stating if date_start and date_end are from
        bounds or from a time variable
    tlen is the number of time levels spanned by date_start and date_end
    """

    # give up if there is 1 time level and no time bounds variable
    if not time_bounds and tlen == 1:
        return "unknown"

    # convert datetime object to numerical values
    units = "days since 0001-01-01 00:00:00"
    days_start = cftime.date2num(date_to_datetime(date_start), units, calendar=calendar)
    days_end = cftime.date2num(date_to_datetime(date_end), units, calendar=calendar)

    if time_bounds:
        dt_avg = (days_end - days_start) / tlen
    else:
        dt_avg = (days_end - days_start) / (tlen - 1)

    eps = 1.0e-3

    if dt_avg < 1 - eps:
        # sub-daily, check for multiples of hourly
        dt_avg *= 24
        for n in range(1, 24):
            if abs(dt_avg - n) < eps:
                return f"hour_{n}"
        return "unknown"
    if dt_avg < 28 - eps:
        # sub-monthly, check for multiples of daily
        for n in range(1, 28):
            if abs(dt_avg - n) < eps:
                return f"day_{n}"
        return "unknown"
    if dt_avg >= 28 - eps and dt_avg <= 31 + eps:
        return "month_1"
    if dt_avg >= 59 - eps and dt_avg <= 62 + eps:
        return "month_2"
    if dt_avg >= 89 - eps and dt_avg <= 92 + eps:
        return "month_3"
    if dt_avg >= 120 - eps and dt_avg <= 123 + eps:
        return "month_4"
    if dt_avg >= 181 - eps and dt_avg <= 184 + eps:
        return "month_6"
    if dt_avg >= 365 - eps and dt_avg <= 366 + eps:
        return "year_1"
    if dt_avg >= 365 * 10 - eps and dt_avg <= 365 * 10 + 3 + eps:
        return "year_10"

    return "unknown"
