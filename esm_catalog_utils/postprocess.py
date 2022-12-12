from inspect import signature

import numpy as np
import xarray as xr


# TODO: add postprocess_cam, which does the following
#   1) add AREA to ds if not available
#      Q: Is there a better option for rearth than using CIME's shr value?
#   2) add cell_measures attribute with "area: AREA" to vars on lat, lon dimensions
#   3) add axis attributes to time, lat, lon, lev, ilev


# TODO: add dt, in seconds, to all postprocess functions


def open_mfdataset_kwargs(scomp):
    """return dict of arguments for open_mfdataset appropriate for scomp"""

    if scomp == "mom6":
        # the following variables can have a CF-compliant units with no
        # calendar attribute, causing problems with time conversion
        return {"drop_variables": ["average_T1", "average_T2"]}
    else:
        return {}


def postprocess(ds, scomp, **kwargs):
    """call postprocessing function appropriate for scomp on ds"""

    if scomp == "pop":
        pp_fcn = postprocess_pop
    elif scomp == "cice":
        pp_fcn = postprocess_cice
    elif scomp == "mom6":
        import functools

        pp_fcn = functools.partial(postprocess_mom6, catalog=..., case=...)
    else:
        return ds

    # construct args to post processing function
    kwargs_avail = {"ds": ds}
    kwargs_avail.update(kwargs)
    pp_kwargs = {arg: kwargs_avail[arg] for arg in signature(pp_fcn).parameters}

    return pp_fcn(**pp_kwargs)


def postprocess_pop(ds):
    """
    POP specific Dataset postprocessing
    add area to cell_measures attribute
    add axis attributes to coordinates
    add nlon, nlat coordinates
    set time to average of time:bounds
    """
    for varname in ds.data_vars:
        if "coordinates" in ds[varname].encoding:
            var_coords = ds[varname].encoding["coordinates"]
            if ("TLONG" in var_coords) and ("TLAT" in var_coords):
                ds[varname].attrs["cell_measures"] = "area: TAREA"
            if ("ULONG" in var_coords) and ("ULAT" in var_coords):
                ds[varname].attrs["cell_measures"] = "area: UAREA"
    ds["time"].attrs["axis"] = "T"
    for coordname in ds.coords:
        if "depth" in ds[coordname].attrs["long_name"]:
            ds[coordname].attrs["axis"] = "Z"
    ds["nlat"] = ("nlat", np.arange(ds.sizes["nlat"]), {"axis": "Y"})
    ds["nlon"] = ("nlon", np.arange(ds.sizes["nlon"]), {"axis": "X"})
    return ds


def postprocess_cice(ds):
    """
    CICE specific Dataset postprocessing
    add axis attributes to coordinates
    add nlon, nlat coordinates
    set time to average of time:bounds
    """
    ds["time"].attrs["axis"] = "T"
    ds["nj"] = ("nj", np.arange(ds.sizes["nj"]), {"axis": "Y"})
    ds["ni"] = ("ni", np.arange(ds.sizes["ni"]), {"axis": "X"})
    return ds


def postprocess_mom6(ds, catalog, case):
    """
    MOM6 specific Dataset postprocessing
    change time:calendar to lower case
    add axis attribute to coordinates
    add data_vars from corresponding static stream to dataset
    add coordinates to data variables
    """
    tb_name = ds["time"].bounds
    for varname in ["time", tb_name]:
        for d in [ds[varname].attrs, ds[varname].encoding]:
            if "calendar" in d:
                d["calendar"] = d["calendar"].lower()
            for att_name in ["calendar_type", "_FillValue", "missing_value"]:
                if att_name in d:
                    del d[att_name]
    for coordname in ds.coords:
        if "cartesian_axis" in ds[coordname].attrs:
            ds[coordname].attrs["axis"] = ds[coordname].attrs["cartesian_axis"]
            del ds[coordname].attrs["cartesian_axis"]
    df = catalog.df
    df = df[df["case"] == case]
    df = df[df["scomp"] == "mom6"]
    df = df[df["stream"] == "static"]
    if len(df) == 0:
        raise ValueError(f"no static stream for {case} found in catalog")
    ds_static = xr.open_dataset(df["path"].values[0])
    for varname in ds_static.data_vars:
        ds[varname] = ds_static[varname]
    for varname in ds.data_vars:
        dims = ds[varname].dims
        if "xh" in dims:
            if "yh" in dims:
                ds[varname].attrs["coordinates"] = "geolat geolon"
            if "yq" in dims:
                ds[varname].attrs["coordinates"] = "geolat_v geolon_v"
        if "xq" in dims:
            if "yh" in dims:
                ds[varname].attrs["coordinates"] = "geolat_u geolon_u"
            if "yq" in dims:
                ds[varname].attrs["coordinates"] = "geolat_c geolon_c"
    return ds
