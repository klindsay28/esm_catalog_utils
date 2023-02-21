import pytest

from esm_catalog_utils import parse_path_cesm


@pytest.mark.parametrize("case", ["casename", "case_w_underscore", "case.w.period"])
def test_parse_path_cesm_time_invariant(case):
    ret_val = parse_path_cesm(f"{case}.pop.once.nc", case)
    assert ret_val == {
        "scomp": "pop",
        "component": "ocn",
        "stream": "once",
        "datestring": "",
    }

    ret_val = parse_path_cesm(f"{case}.mom6.static.nc", case)
    assert ret_val == {
        "scomp": "mom6",
        "component": "ocn",
        "stream": "static",
        "datestring": "",
    }


@pytest.mark.parametrize("case", ["casename", "case_w_underscore", "case.w.period"])
@pytest.mark.parametrize(
    "stream", ["h", "h.nday1", "h.ecosys.nday1", "h_bgc", "h_bgc_z"]
)
@pytest.mark.parametrize("datestring", ["1850", "1850-01", "1850_01"])
def test_parse_path_cesm_hist(case, stream, datestring):
    ret_val = parse_path_cesm(f"{case}.pop.{stream}.{datestring}.nc", case)
    assert ret_val == {
        "scomp": "pop",
        "component": "ocn",
        "stream": stream,
        "datestring": datestring,
    }

    ret_val = parse_path_cesm(f"{case}.mom6.{stream}_{datestring}.nc", case)
    assert ret_val == {
        "scomp": "mom6",
        "component": "ocn",
        "stream": stream,
        "datestring": datestring,
    }


@pytest.mark.parametrize("case", ["casename", "case_w_underscore", "case.w.period"])
@pytest.mark.parametrize(
    "stream", ["h", "h.nday1", "h.ecosys.nday1", "h_bgc", "h_bgc_z"]
)
@pytest.mark.parametrize("varname", ["TEMP", "FG_CO2"])
@pytest.mark.parametrize(
    "daterange", ["1850-1899", "185001-189912", "18500101-18991231"]
)
def test_parse_path_cesm_tseries(case, stream, varname, daterange):
    ret_val = parse_path_cesm(f"{case}.pop.{stream}.{varname}.{daterange}.nc", case)
    assert ret_val == {
        "scomp": "pop",
        "component": "ocn",
        "stream": stream,
        "varname": varname,
        "datestring": daterange,
    }

    ret_val = parse_path_cesm(f"{case}.mom6.{stream}.{varname}_{daterange}.nc", case)
    assert ret_val == {
        "scomp": "mom6",
        "component": "ocn",
        "stream": stream,
        "varname": varname,
        "datestring": daterange,
    }

    daterange = daterange.replace("-", "_")
    ret_val = parse_path_cesm(f"{case}.mom6.{stream}.{varname}_{daterange}.nc", case)
    assert ret_val == {
        "scomp": "mom6",
        "component": "ocn",
        "stream": stream,
        "varname": varname,
        "datestring": daterange,
    }
