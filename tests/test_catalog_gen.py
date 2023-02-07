import json
import os.path

from dask.distributed import Client
import pandas as pd
import pytest

from esm_catalog_utils.catalog_gen import case_metadata_to_esm_datastore, read_catalog
from gen_test_input import gen_test_input


def dict_cmp(d1, d2, ignore_keys=None):
    """compare dictionaries, ignoring keys in ignore_keys"""

    d1_subset = {key: d1[key] for key in d1 if key not in ignore_keys}
    d2_subset = {key: d2[key] for key in d2 if key not in ignore_keys}
    return d1_subset == d2_subset


@pytest.mark.parametrize("parallel", [False, True])
def test_gen_esmcol_files(parallel):
    repo_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    test_input_dir = os.path.join(repo_root, "tests", "test_input_files")
    baseline_dir = os.path.join(repo_root, "tests", "catalog_baselines")

    cases_metadata = gen_test_input(test_input_dir)

    if parallel:
        # avoid threads because of
        # https://github.com/Unidata/netcdf4-python/issues/1192
        client = Client(n_workers=2, threads_per_worker=1)

    for case_metadata in cases_metadata:
        # generate esm_datastore object from case_metadata
        esm_datastore = case_metadata_to_esm_datastore(case_metadata)

        # write esm_datastore object to disk
        case = case_metadata["case"]
        esm_datastore.serialize(
            name=case, directory=test_input_dir, catalog_type="file"
        )

        # compare generated csv file to baseline
        # after replacing REPO_ROOT with repo_root in path
        fname = f"{case}.csv"
        baseline = read_catalog(os.path.join(baseline_dir, fname))
        generated = read_catalog(os.path.join(test_input_dir, fname))
        for key in baseline:
            if key != "path":
                assert (baseline[key] == generated[key]).all()
            else:
                baseline_replace = pd.Series(
                    [foo.replace("REPO_ROOT", repo_root) for foo in baseline[key]]
                )
                assert (baseline_replace == generated[key]).all()

        # compare generated json file to baseline, ignoring last_updated key
        fname = f"{case}.json"
        with open(os.path.join(baseline_dir, fname), mode="r") as fptr:
            baseline = json.load(fptr)
        with open(os.path.join(test_input_dir, fname), mode="r") as fptr:
            generated = json.load(fptr)

        assert dict_cmp(baseline, generated, ignore_keys=["last_updated"])

    if parallel:
        client.close()
