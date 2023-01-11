import pytest

from esm_catalog_utils.catalog_gen import path_to_attrs
from gen_test_input import gen_test_input


def test_path_to_attrs_time_invariant():
    ret_val = path_to_attrs("case.pop.once.nc", "case", "pop")
    assert ret_val == {"stream": "once"}

    ret_val = path_to_attrs("case.mom6.static.nc", "case", "mom6")
    assert ret_val == {"stream": "static"}


@pytest.mark.parametrize(
    "stream", ["h", "h.nday1", "h.ecosys.nday1", "h_bgc", "h_bgc_z"]
)
@pytest.mark.parametrize("datestring", ["1850", "1850-01", "1850_01"])
def test_path_to_attrs_hist(stream, datestring):
    ret_val = path_to_attrs(f"case.pop.{stream}.{datestring}.nc", "case", "pop")
    assert ret_val == {"stream": stream, "datestring": datestring}

    ret_val = path_to_attrs(f"case.mom6.{stream}_{datestring}.nc", "case", "mom6")
    assert ret_val == {"stream": stream, "datestring": datestring}


@pytest.mark.parametrize(
    "stream", ["h", "h.nday1", "h.ecosys.nday1", "h_bgc", "h_bgc_z"]
)
@pytest.mark.parametrize("varname", ["TEMP", "FG_CO2"])
@pytest.mark.parametrize(
    "daterange", ["1850-1899", "185001-189912", "18500101-18991231"]
)
def test_path_to_attrs_tseries(stream, varname, daterange):
    ret_val = path_to_attrs(
        f"case.pop.{stream}.{varname}.{daterange}.nc", "case", "pop"
    )
    assert ret_val == {"stream": stream, "varname": varname, "datestring": daterange}

    ret_val = path_to_attrs(
        f"case.mom6.{stream}.{varname}_{daterange}.nc", "case", "mom6"
    )
    assert ret_val == {"stream": stream, "varname": varname, "datestring": daterange}

    daterange = daterange.replace("-", "_")
    ret_val = path_to_attrs(
        f"case.mom6.{stream}.{varname}_{daterange}.nc", "case", "mom6"
    )
    assert ret_val == {"stream": stream, "varname": varname, "datestring": daterange}


def test_gen_esmcol_files():
    gen_test_input()
