#!/usr/bin/env python
"""generate input files for testing catalog generation"""

import os.path
import pprint

from xr_ds_ex import xr_ds_ex


def ym_date_str(tval, sep="-"):
    return f"{tval.year:04d}{sep}{tval.month:02d}"


def gen_test_input(root_dir=None, casename="case"):
    """
    generate input files for testing catalog generation
    return case metadata appropriate for generated input files
    """

    scomp_dict = {"atm": "cam", "lnd": "clm2"}
    stream = "h0"

    # create input file subdirectories

    if root_dir is None:
        repo_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        root_dir = os.path.join(repo_root, "tests", "generated", casename)
    for comp in scomp_dict:
        dirname = os.path.join(root_dir, comp)
        os.makedirs(dirname, exist_ok=True)
        for file_type in ["hist", "tseries"]:
            dirname = os.path.join(root_dir, comp, file_type)
            os.makedirs(dirname, exist_ok=True)

    for comp, scomp in scomp_dict.items():
        # generate synthetic dataset

        ds = xr_ds_ex(var_const=False, var_name=f"{comp}_var1")
        ds.encoding["unlimited_dims"] = "time"
        ds[f"{comp}_var2"] = ds[f"{comp}_var1"] * ds[f"{comp}_var1"]
        ds[f"{comp}_var3"] = ds[f"{comp}_var1"] * ds[f"{comp}_var2"]
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

        for time_ind in range(len(ds["time"])):
            date_str = ym_date_str(ds["time"].values[time_ind])
            fname = f"{casename}.{scomp}.{stream}.{date_str}.nc"
            path = os.path.join(root_dir, comp, "hist", fname)
            ds.isel(time=slice(time_ind, time_ind + 1)).to_netcdf(path)

        # generate corresponding tseries files

        date_str_lo = ym_date_str(ds["time"].values[0], sep="")
        date_str_hi = ym_date_str(ds["time"].values[11], sep="")
        for varname in ds.data_vars:
            if varname == "time_bounds":
                continue
            fname = (
                f"{casename}.{scomp}.{stream}.{varname}.{date_str_lo}-{date_str_hi}.nc"
            )
            path = os.path.join(root_dir, comp, "tseries", fname)
            ds[["time_bounds", varname]].isel(time=slice(0, 12)).to_netcdf(path)

        date_str_lo = ym_date_str(ds["time"].values[12], sep="")
        date_str_hi = ym_date_str(ds["time"].values[-1], sep="")
        for varname in ds.data_vars:
            if varname == "time_bounds":
                continue
            fname = (
                f"{casename}.{scomp}.{stream}.{varname}.{date_str_lo}-{date_str_hi}.nc"
            )
            path = os.path.join(root_dir, comp, "tseries", fname)
            ds[["time_bounds", varname]].isel(time=slice(12, 37)).to_netcdf(path)

    ret_val = []
    for file_type in ["hist", "tseries"]:
        output_dirs = [os.path.join(root_dir, comp, file_type) for comp in scomp_dict]
        ret_val.append({"case": casename, "output_dirs": output_dirs})

    return ret_val


if __name__ == "__main__":
    cases_metadata = gen_test_input()
    pprint.pprint(cases_metadata)
