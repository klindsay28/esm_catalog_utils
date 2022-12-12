"""
utilities to access CIME functionality
"""

import subprocess


def cime_xmlquery(caseroot, varname):
    """run CIME's xmlquery for varname in the directory caseroot, return the value"""
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
