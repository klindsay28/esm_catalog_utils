#!/usr/bin/env python
"""generate input files for testing catalog generation"""

import os.path
import pprint

from xr_ds_ex import xr_ds_ex


def gen_date_str(tval, freq, sep="-"):
    if freq.startswith("year_"):
        return f"{tval.year:04d}"
    if freq.startswith("month_"):
        return f"{tval.year:04d}{sep}{tval.month:02d}"
    if freq.startswith("day_"):
        return f"{tval.year:04d}{sep}{tval.month:02d}{sep}{tval.day:02d}"
    raise ValueError(f"unsupported freq={freq}")


def gen_test_input(root_dir=None, casename="case"):
    """
    generate input files for testing catalog generation
    return case metadata appropriate for generated input files
    """

    scomp_dict = {"atm": "cam", "lnd": "clm2"}
    stream_freq_dict = {"h0": "month_1", "h1": "day_10"}
    nyrs = 4

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
        for stream, freq in stream_freq_dict.items():
            # generate synthetic dataset

            ds = xr_ds_ex(
                nyrs=nyrs, var_const=False, var_name=f"{comp}_var1", freq=freq
            )
            ds.encoding["unlimited_dims"] = "time"
            ds[f"{comp}_var2"] = ds[f"{comp}_var1"] * ds[f"{comp}_var1"]
            ds[f"{comp}_var3"] = ds[f"{comp}_var1"] * ds[f"{comp}_var2"]
            # add time_period_freq for some, but not all, components,
            # to increase coverage of testing
            if scomp == "cam":
                ds.attrs["time_period_freq"] = freq

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

            tb_name = ds["time"].attrs["bounds"]

            # generate corresponding hist files

            for time_ind in range(len(ds["time"])):
                if freq.startswith("day_"):
                    date_str = gen_date_str(ds[tb_name].values[time_ind, 1], freq)
                else:
                    date_str = gen_date_str(ds["time"].values[time_ind], freq)
                fname = f"{casename}.{scomp}.{stream}.{date_str}.nc"
                path = os.path.join(root_dir, comp, "hist", fname)
                ds.isel(time=slice(time_ind, time_ind + 1)).to_netcdf(path)

            # generate corresponding tseries files

            tlen_subset = len(ds.sel(time=slice("0001-01-01", "0002-01-01"))["time"])

            for time_slice in [slice(0, tlen_subset), slice(tlen_subset, None)]:
                ds_subset = ds.isel(time=time_slice)

                if freq.startswith("day_"):
                    date_str_lo = gen_date_str(
                        ds_subset[tb_name].values[0, 0], freq, sep=""
                    )
                    date_str_hi = gen_date_str(
                        ds_subset[tb_name].values[-1, 1], freq, sep=""
                    )
                else:
                    date_str_lo = gen_date_str(
                        ds_subset["time"].values[0], freq, sep=""
                    )
                    date_str_hi = gen_date_str(
                        ds_subset["time"].values[-1], freq, sep=""
                    )
                date_str = f"{date_str_lo}-{date_str_hi}"

                for varname in ds_subset.data_vars:
                    if varname == "time_bounds":
                        continue
                    fname = f"{casename}.{scomp}.{stream}.{varname}.{date_str}.nc"
                    path = os.path.join(root_dir, comp, "tseries", fname)
                    ds_subset[["time_bounds", varname]].to_netcdf(path)

    ret_val = []
    for file_type in ["hist", "tseries"]:
        output_dirs = [os.path.join(root_dir, comp, file_type) for comp in scomp_dict]
        ret_val.append({"case": casename, "output_dirs": output_dirs})

    return ret_val


if __name__ == "__main__":
    cases_metadata = gen_test_input()
    pprint.pprint(cases_metadata)
