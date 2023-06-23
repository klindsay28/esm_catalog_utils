"""Utilities to access CIME functionality."""

import subprocess
from os import PathLike
from typing import Union


def cime_xmlquery(caseroot: Union[str, PathLike], varname: str) -> str:
    """
    Query a CIME XML variable for its value.

    Parameters
    ----------
    caseroot : str or path-like
        Caseroot directory of case being queried.
    varname : str
        Name of variable being queried.

    Returns
    -------
    str
        Value corresponding to `varname`.
    """
    try:
        value = subprocess.check_output(
            ["./xmlquery", "-N", "--value", varname],
            stderr=subprocess.STDOUT,
            cwd=caseroot,
        )
    except subprocess.CalledProcessError:
        value = subprocess.check_output(
            ["./xmlquery", "--value", varname], stderr=subprocess.STDOUT, cwd=caseroot
        )
    return value.decode()
