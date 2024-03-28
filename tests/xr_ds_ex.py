"""function for example xarray.Dataset object"""

import cftime
import numpy as np
import xarray as xr

days_1yr = np.array(
    [31.0, 28.0, 31.0, 30.0, 31.0, 30.0, 31.0, 31.0, 30.0, 31.0, 30.0, 31.0]
)


def gen_monthly_time_edge_values(nyrs=3):
    """return numpy array of edges of month boundaries"""
    return np.insert(np.cumsum(np.tile(days_1yr, nyrs)), 0, 0)


def xr_ds_ex(
    nyrs=3,
    time_mid=True,
    decode_times=True,
    var_const=True,
    var_name="var",
    freq="month_1",
):
    """return an example xarray.Dataset object, useful for testing functions"""

    # set up values for Dataset, nyrs yrs of analytic values
    if freq.startswith("month_"):
        time_edge_values = gen_monthly_time_edge_values(nyrs)
    elif freq.startswith("day_"):
        time_edge_values = np.arange(nyrs * 365 + 1)
    elif freq.startswith("year_"):
        time_edge_values = 365 * np.arange(nyrs + 1)
    else:
        raise ValueError(f"unsupported freq={freq}")

    samp_freq = int(freq.split("_")[-1])
    if (len(time_edge_values) - 1) % samp_freq != 0:
        raise ValueError(
            f"sampling freq={samp_freq} must divide len={len(time_edge_values) - 1}"
        )
    time_edge_values = time_edge_values[::samp_freq]

    time_bounds_values = np.stack((time_edge_values[:-1], time_edge_values[1:]), axis=1)

    if time_mid:
        time_values = 0.5 * (time_bounds_values[:, 0] + time_bounds_values[:, 1])
    else:
        time_values = time_bounds_values[:, 1]
    time_values_yr = time_values / 365.0
    if var_const:
        var_values = np.ones_like(time_values_yr)
    else:
        var_values = np.sin(np.pi * time_values_yr) * np.exp(-0.1 * time_values_yr)

    time_units = "days since 0001-01-01"
    calendar = "noleap"

    if decode_times:
        time_values = cftime.num2date(time_values, time_units, calendar)
        time_bounds_values = cftime.num2date(time_bounds_values, time_units, calendar)

    # create Dataset, including time_bounds
    time_var = xr.DataArray(
        time_values,
        name="time",
        dims="time",
        coords={"time": time_values},
        attrs={"bounds": "time_bounds"},
    )
    if not decode_times:
        time_var.attrs["units"] = time_units
        time_var.attrs["calendar"] = calendar
    time_bounds = xr.DataArray(
        time_bounds_values,
        name="time_bounds",
        dims=("time", "d2"),
        coords={"time": time_var},
    )
    var = xr.DataArray(
        var_values, name=var_name, dims="time", coords={"time": time_var}
    )
    ds = var.to_dataset()
    ds = xr.merge([ds, time_bounds])

    if decode_times:
        ds.time.encoding["units"] = time_units
        ds.time.encoding["calendar"] = calendar

    return ds
