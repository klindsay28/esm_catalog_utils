#!/usr/bin/env python
"""print case metadata dict from xml files in provided caseroot"""

import argparse
import os.path
import sys

import yaml

from esm_catalog_utils.cime import cime_xmlquery


def caseroot_to_case_metadata(caseroot):
    """return case metadata dict from xml files in provided caseroot"""

    case_metadata = {}
    case = cime_xmlquery(caseroot, "CASE")
    case_metadata["case"] = case
    dout_s = cime_xmlquery(caseroot, "DOUT_S").upper() == "TRUE"
    if not dout_s:
        case_metadata["output_dirs"] = [cime_xmlquery(caseroot, "RUNDIR")]
    else:
        dout_s_root = cime_xmlquery(caseroot, "DOUT_S_ROOT")
        output_dirs = []
        for gcomp in cime_xmlquery(caseroot, "COMP_CLASSES").split(","):
            path = os.path.join(dout_s_root, gcomp.lower(), "hist")
            if os.path.exists(path):
                output_dirs.append(path)
        case_metadata["output_dirs"] = output_dirs
    return case_metadata


def parse_args(args):
    """parse command line arguments"""

    parser = argparse.ArgumentParser(
        description="print case metadata dict from xml files in provided caseroot",
    )
    parser.add_argument(
        "caseroot",
        help="caseroot directory for case that metadata is being extracted from",
    )
    return parser.parse_args()


def main(args):
    """execute caseroot_to_case_metadata based on command line arguments"""

    case_metadata = caseroot_to_case_metadata(args.caseroot)
    print(yaml.dump([case_metadata], sort_keys=False))


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
