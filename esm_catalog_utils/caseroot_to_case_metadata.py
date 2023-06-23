#!/usr/bin/env python
"""Print case metadata dict from xml files in provided caseroot."""

import argparse
import os.path
import pprint
import sys
from os import PathLike
from typing import Any, Dict, List, Union

from esm_catalog_utils.cime import cime_xmlquery


def query_from_caseroot(caseroot: Union[str, PathLike], varname: str) -> str:
    """
    Query the value of varname from caseroot.

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
    return cime_xmlquery(caseroot, varname)


def caseroot_to_case_metadata(caseroot: Union[str, PathLike]) -> Dict[str, Any]:
    """
    Generate case metadata dict from a CIME case's xml files.

    The returned dict has key-value pairs for the following keys:

    - "case": Name of case in `caseroot`.
    - "output_dirs": List of directories where output from `case` is located.

    Parameters
    ----------
    caseroot : str or path-like
        Caseroot directory of case being queried.

    Returns
    -------
    dict

    See Also
    --------
    case_metadata_to_esm_datastore
    """

    case_metadata: Dict[str, Any] = {}
    case = cime_xmlquery(caseroot, "CASE")
    case_metadata["case"] = case
    dout_s = cime_xmlquery(caseroot, "DOUT_S").upper() == "TRUE"
    if not dout_s:
        case_metadata["output_dirs"] = [cime_xmlquery(caseroot, "RUNDIR")]
    else:
        dout_s_root = cime_xmlquery(caseroot, "DOUT_S_ROOT")
        output_dirs: List[Union[str, PathLike]] = []
        for gcomp in cime_xmlquery(caseroot, "COMP_CLASSES").split(","):
            path = os.path.join(dout_s_root, gcomp.lower(), "hist")
            if os.path.exists(path):
                output_dirs.append(path)
        case_metadata["output_dirs"] = output_dirs
    return case_metadata


def parse_args(args: List[str]) -> argparse.Namespace:
    """
    Parse command line arguments.

    Parameters
    ----------
    args : list of str
        Command line arguments.

    Returns
    -------
    argparse.Namespace
        Object of parsed command line arguments.
    """

    parser = argparse.ArgumentParser(
        description="print case metadata dict from xml files in provided caseroot",
    )
    parser.add_argument(
        "caseroot",
        help="caseroot directory for case that metadata is being extracted from",
    )
    return parser.parse_args()


def main(args: argparse.Namespace) -> None:
    """
    Execute caseroot_to_case_metadata based on command line arguments.

    Parameters
    ----------
    args : argparse.Namespace
        Object of parsed command line arguments.
    """

    case_metadata = caseroot_to_case_metadata(args.caseroot)
    pprint.pprint(case_metadata)


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
