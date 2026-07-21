"""Tests for cli.py's Dataflow-worker-packaging preflight check — pure
argparse-level logic, no Beam pipeline needed."""

import pytest

from streaming_pipeline_framework.cli import _check_dataflow_worker_packaging


class TestCheckDataflowWorkerPackaging:
    def test_direct_runner_never_raises(self):
        _check_dataflow_worker_packaging("DirectRunner", [])

    def test_dataflow_runner_without_packaging_flag_raises(self):
        with pytest.raises(SystemExit, match="streaming_pipeline_framework staged onto workers"):
            _check_dataflow_worker_packaging("DataflowRunner", [])

    def test_dataflow_runner_with_unrelated_flags_raises(self):
        with pytest.raises(SystemExit):
            _check_dataflow_worker_packaging("DataflowRunner", ["--max_num_workers=5"])

    @pytest.mark.parametrize(
        "flag", ["--extra_package", "--setup_file", "--sdk_location", "--sdk_container_image"]
    )
    def test_dataflow_runner_with_space_separated_flag_passes(self, flag):
        _check_dataflow_worker_packaging("DataflowRunner", [flag, "some/value"])

    @pytest.mark.parametrize(
        "flag", ["--extra_package", "--setup_file", "--sdk_location", "--sdk_container_image"]
    )
    def test_dataflow_runner_with_equals_form_flag_passes(self, flag):
        _check_dataflow_worker_packaging("DataflowRunner", [f"{flag}=some/value"])

    def test_dataflow_runner_flag_among_other_args_passes(self):
        _check_dataflow_worker_packaging(
            "DataflowRunner", ["--max_num_workers=5", "--extra_package", "dist/foo.whl"]
        )
