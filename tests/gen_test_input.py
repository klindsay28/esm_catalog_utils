#!/usr/bin/env python
"""generate input files for testing catalog generation"""

import os.path

from xr_ds_ex import xr_ds_ex


def ym_date_str(tval, sep="-"):
    return f"{tval.year:04d}{sep}{tval.month:02d}"


def gen_test_input(test_input_dir=None):
    """
    generate input files for testing catalog generation
    return case metadata appropriate for generated input files
    """

    # create input file subdirectories

    if test_input_dir is None:
        repo_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        test_input_dir = os.path.join(repo_root, "tests", "test_input_files")
    hist_input_dir = os.path.join(test_input_dir, "hist")
    tseries_input_dir = os.path.join(test_input_dir, "tseries")
    for dirname in [hist_input_dir, tseries_input_dir]:
        os.makedirs(dirname, exist_ok=True)

    # generate synthetic dataset

    ds = xr_ds_ex(var_const=False, var_name="var1")
    ds.encoding["unlimited_dims"] = "time"
    ds["var2"] = ds["var1"] * ds["var1"]
    ds["var3"] = ds["var1"] * ds["var2"]
    ds.attrs["time_period_freq"] = "month_1"

    # cleanup metadata

    for varname in ds.variables:
        if "_FillValue" not in ds[varname].encoding:
            ds[varname].encoding["_FillValue"] = None
        if varname in ["time", "time_bounds"]:
            ds[varname].encoding["dtype"] = "float64"
        else:
            ds[varname].encoding["dtype"] = "float32"
            ds[varname].attrs["long_name"] = varname
            ds[varname].attrs["units"] = "m"

    # generate corresponding hist files

    histcase = "histcase"
    scomp = "cam"
    stream = "h0"

    for time_ind in range(len(ds["time"])):
        date_str = ym_date_str(ds["time"].values[time_ind])
        fname = f"{histcase}.{scomp}.{stream}.{date_str}.nc"
        path = os.path.join(hist_input_dir, fname)
        ds.isel(time=slice(time_ind, time_ind + 1)).to_netcdf(path)

    # generate corresponding tseries files

    tseriescase = "tseriescase"
    scomp = "clm2"
    stream = "h0"

    date_str_lo = ym_date_str(ds["time"].values[0], sep="")
    date_str_hi = ym_date_str(ds["time"].values[11], sep="")
    for varname in ds.data_vars:
        if varname == "time_bounds":
            continue
        fname = (
            f"{tseriescase}.{scomp}.{stream}.{varname}.{date_str_lo}-{date_str_hi}.nc"
        )
        path = os.path.join(tseries_input_dir, fname)
        ds[["time_bounds", varname]].isel(time=slice(0, 12)).to_netcdf(path)

    date_str_lo = ym_date_str(ds["time"].values[12], sep="")
    date_str_hi = ym_date_str(ds["time"].values[-1], sep="")
    for varname in ds.data_vars:
        if varname == "time_bounds":
            continue
        fname = (
            f"{tseriescase}.{scomp}.{stream}.{varname}.{date_str_lo}-{date_str_hi}.nc"
        )
        path = os.path.join(tseries_input_dir, fname)
        ds[["time_bounds", varname]].isel(time=slice(12, 37)).to_netcdf(path)

    return [
        {"case": histcase, "output_dirs": [hist_input_dir]},
        {"case": tseriescase, "output_dirs": [tseries_input_dir]},
    ]


if __name__ == "__main__":
    gen_test_input()
