"""Functions to create esm_datastore objects in particular use-cases."""

import os.path
from os import PathLike
from typing import Optional, Union

from intake_esm import esm_datastore

from .caseroot_to_case_metadata import caseroot_to_case_metadata
from .catalog_gen import case_metadata_to_esm_datastore


def directory_to_esm_datastore(
    dir: Union[str, PathLike], case: Optional[str] = None, **kwargs
) -> esm_datastore:
    """
    Generate `esm_datastore
    <https://intake-esm.readthedocs.io/en/stable/reference/api.html>`_
    object for files in a directory and its subdirectories.

    Parameters
    ----------
    dir : str or path-like
        Directory containing files to be cataloged.
        Files in subdirectories of `dir` are also cataloged.
    case : str or None, optional
        Name of case that generated the files in dir.
        If `case` is None, `case` is taken to be the basename of dir.
    **kwargs : dict, optional
        Additional keyword arguments passed on to
        :py:func:`case_metadata_to_esm_datastore`.

    Returns
    -------
    esm_datastore

    See Also
    --------
    case_metadata_to_esm_datastore

    Notes
    -----
    Passes a created dictionary of case metadata to
    :py:func:`case_metadata_to_esm_datastore`.
    """

    if case is None:
        case = os.path.basename(dir)
    case_metadata = {"case": case, "output_dirs": [dir]}
    return case_metadata_to_esm_datastore(case_metadata, **kwargs)


def caseroot_to_esm_datastore(
    caseroot: Union[str, PathLike], **kwargs
) -> esm_datastore:
    """
    Generate `esm_datastore
    <https://intake-esm.readthedocs.io/en/stable/reference/api.html>`_
    object for files generated by a case.

    Parameters
    ----------
    caseroot : str or path-like
        Caseroot directory of case that generated files.
    **kwargs : dict, optional
        Additional keyword arguments passed on to
        :py:func:`case_metadata_to_esm_datastore`.

    Returns
    -------
    esm_datastore

    See Also
    --------
    case_metadata_to_esm_datastore

    Notes
    -----
    Passes a created dictionary of case metadata to
    :py:func:`case_metadata_to_esm_datastore`.
    """

    case_metadata = caseroot_to_case_metadata(caseroot)
    return case_metadata_to_esm_datastore(case_metadata, **kwargs)
