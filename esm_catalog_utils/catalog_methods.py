import xarray as xr

from .postprocess import open_mfdataset_kwargs, postprocess


def catalog_sel_to_df(catalog, date_range, case, scomp, stream, varname):
    """create dataframe from catalog specific to other args"""
    df = catalog.df
    # The date_start==date_end conditional in the following conditional is to
    # ensure that MOM6's static stream always gets propagated if present.
    # This is needed for grid metrics.
    # There might be other ways to accomplish this.
    date_mask = (
        (df["date_start"] < date_range[1]) & (df["date_end"] > date_range[0])
    ) | (df["date_start"] == df["date_end"])
    df = df[date_mask]
    df = df[df["case"] == case]
    df = df[df["scomp"] == scomp]
    df = df[df["stream"] == stream]
    inds = [ind for ind, varnames in enumerate(df["varname"]) if varname in varnames]
    if len(inds) == 0:
        return None
    return df.iloc[inds]


def catalog_sel_to_ds(catalog, date_range, case, scomp, stream, varname):
    """create Dataset from catalog specific to other args"""
    df = catalog_sel_to_df(catalog, date_range, case, scomp, stream, varname)
    if df is None:
        return None
    print(f"generating ds, len(df)={len(df)}")
    paths = df["path"].to_list()
    kwargs = {
        "compat": "override",
        "data_vars": "minimal",
        "coords": "minimal",
        "parallel": True,
    }
    kwargs.update(open_mfdataset_kwargs(scomp))
    print("calling open_mfdataset")
    ds = xr.open_mfdataset(paths, **kwargs)
    print("calling postprocess")
    ds = postprocess(ds, scomp, catalog=catalog, case=case)

    if True:
        print("copying metadata from first file")
        # copy metadata not propagated by open_mfdataset from 1st file
        kwargs = open_mfdataset_kwargs(scomp)
        ds0 = xr.open_dataset(paths[0], **kwargs)
        ds0 = postprocess(ds0, scomp, catalog=catalog, case=case)
        ds.attrs = ds0.attrs
        for key in ["unlimited_dims"]:
            if key in ds0.encoding:
                ds.encoding[key] = ds0.encoding[key]
        ds["time"].encoding = ds0["time"].encoding
        for var in ds.data_vars:
            ds[var].encoding = ds0[var].encoding

    return ds
