#!/usr/bin/env python
"""print case metadata dict from xml files in provided caseroot"""

import argparse
import os.path
import sys

import yaml

from .cime import cime_xmlquery


def caseroot_to_case_metadata(caseroot, sname, esmcol_spec_dir):
    """return case metadata dict from xml files in provided caseroot"""

    # dict that translates comp name to name used in hist filenames, if they differ
    scomp = {"cice5": "cice", "clm": "clm2", "mom": "mom6"}

    case_metadata = {}
    case_metadata["sname"] = sname
    case = cime_xmlquery(caseroot, "CASE")
    case_metadata["case"] = case
    case_metadata["esmcol_spec_path"] = os.path.join(esmcol_spec_dir, f"{case}.json")
    dout_s = cime_xmlquery(caseroot, "DOUT_S").upper() == "TRUE"
    if dout_s:
        dout_s_root = cime_xmlquery(caseroot, "DOUT_S_ROOT")
    else:
        rundir = cime_xmlquery(caseroot, "RUNDIR")
    case_metadata["components"] = []
    for gcomp in ["atm", "ice", "lnd", "ocn"]:
        comp_dict = {}
        comp_name = cime_xmlquery(caseroot, f"COMP_{gcomp.upper()}")
        comp_dict["scomp"] = scomp.get(comp_name, comp_name)
        if dout_s:
            comp_dict["histdir"] = os.path.join(dout_s_root, gcomp, "hist")
        else:
            comp_dict["histdir"] = rundir
        case_metadata["components"].append(comp_dict)

    return case_metadata


def parse_args(args):
    """parse command line arguments"""

    parser = argparse.ArgumentParser(
        description="print case metadata dict from xml files in provided caseroot",
    )
    parser.add_argument(
        "--caseroot",
        help="caseroot directory for case that metadata is being extracted from",
    )
    parser.add_argument(
        "--sname",
        help="shortname for case",
    )
    parser.add_argument(
        "--esmcol_spec_dir",
        help="directory where esmcol spec file will reside",
    )
    return parser.parse_args()


def main(args):
    """execute caseroot_to_case_metadata based on command line arguments"""

    case_metadata = caseroot_to_case_metadata(
        args.caseroot, args.sname, args.esmcol_spec_dir
    )
    print(yaml.dump([case_metadata], sort_keys=False))


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
