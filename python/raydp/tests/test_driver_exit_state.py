import json
import os
import signal
import subprocess
import textwrap
import time
from pathlib import Path

import ray
import pytest


FINISHED_SENTINEL = "RAYDP_E2E_FINISHED"
FAILED_SENTINEL = "RAYDP_E2E_INTENTIONAL_FAILURE"
KILLED_RUNNING_SENTINEL = "RAYDP_E2E_DRIVER_RUNNING"
KILL_AFTER_SECONDS = 30

# The scenario is passed by environment instead of as a script argument because
# bin/raydp-submit injects its own Spark --conf entries before the last argument.
DRIVER_SCRIPT = """
import os
import time
from pyspark.sql import SparkSession

scenario = os.environ["RAYDP_E2E_SCENARIO"]
spark = SparkSession.builder.appName("raydp-submit-state-e2e-" + scenario).getOrCreate()

try:
    # Run a real Spark action before reporting the sentinel so each case covers
    # the raydp-submit -> Spark driver -> RayDP AppMaster path.
    assert spark.range(0, 10).count() == 10

    if scenario == "finished":
        print("RAYDP_E2E_FINISHED", flush=True)
    elif scenario == "failed":
        print("RAYDP_E2E_INTENTIONAL_FAILURE", flush=True)
        raise RuntimeError("RAYDP_E2E_INTENTIONAL_FAILURE")
    elif scenario == "killed":
        # Keep the driver alive long enough for pytest to terminate raydp-submit
        # with a real external signal.
        print("RAYDP_E2E_DRIVER_RUNNING", flush=True)
        time.sleep(300)
    else:
        raise ValueError("unknown scenario: " + scenario)
finally:
    spark.stop()
"""


@pytest.fixture(scope="module")
def ray_conf_path(tmp_path_factory):
    # raydp-submit needs the same cluster metadata that raydp-submit normally
    # receives in production. Reuse the Ray head started by Docker/CI.
    started_ray_client = False
    if not ray.is_initialized():
        ray.init(address="auto")
        started_ray_client = True
    try:
        node = ray.worker.global_worker.node
        options = {
            "ray": {
                "run-mode": "CLUSTER",
                "node-ip": node.node_ip_address,
                "address": node.address,
                "session-dir": node.get_session_dir_path(),
            }
        }
    finally:
        if started_ray_client:
            ray.shutdown()

    conf_path = tmp_path_factory.mktemp("ray-conf") / "ray.conf"
    conf_path.write_text(json.dumps(options), encoding="utf-8")
    return conf_path


@pytest.fixture(scope="module")
def driver_script_path(tmp_path_factory):
    # Keep the driver app as a temp file so the test exercises bin/raydp-submit
    # exactly like a user-submitted Python application.
    script_path = tmp_path_factory.mktemp("raydp-submit-state") / "driver_state_app.py"
    script_path.write_text(textwrap.dedent(DRIVER_SCRIPT), encoding="utf-8")
    return script_path


def _repo_root():
    return Path(__file__).resolve().parents[3]


def _raydp_submit_command(ray_conf_path, driver_script_path):
    # Use the smallest fixed Spark cluster shape that can run the action. The
    # driver script must remain the final argument for bin/raydp-submit.
    return [
        str(_repo_root() / "bin" / "raydp-submit"),
        "--ray-conf",
        str(ray_conf_path),
        "--conf",
        "spark.executor.cores=1",
        "--conf",
        "spark.executor.instances=1",
        "--conf",
        "spark.executor.memory=500m",
        "--conf",
        "spark.dynamicAllocation.enabled=false",
        "--conf",
        "spark.ui.enabled=false",
        str(driver_script_path),
    ]


def _subprocess_env(scenario):
    env = os.environ.copy()
    # Avoid inheriting PySpark launcher overrides from the outer test process.
    env.pop("PYSPARK_DRIVER_PYTHON", None)
    env.pop("PYSPARK_PYTHON", None)
    env["RAYDP_E2E_SCENARIO"] = scenario
    return env


def _run_raydp_submit(ray_conf_path, driver_script_path, scenario):
    return subprocess.run(
        _raydp_submit_command(ray_conf_path, driver_script_path),
        cwd=str(_repo_root()),
        env=_subprocess_env(scenario),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=240,
    )


@pytest.mark.parametrize(
    "scenario, expected_returncode, sentinel",
    [
        ("finished", 0, FINISHED_SENTINEL),
        ("failed", 1, FAILED_SENTINEL),
    ],
)
def test_raydp_submit_terminal_state(
        ray_conf_path, driver_script_path, scenario, expected_returncode, sentinel):
    result = _run_raydp_submit(ray_conf_path, driver_script_path, scenario)

    # This smoke test validates user-visible terminal behavior: success exits 0,
    # failure exits non-zero, and the driver reached the intended branch.
    if expected_returncode == 0:
        assert result.returncode == 0, result.stdout
    else:
        assert result.returncode != 0, result.stdout
    assert sentinel in result.stdout


def test_raydp_submit_killed_smoke(ray_conf_path, driver_script_path, tmp_path):
    output_path = tmp_path / "raydp-submit-killed.log"
    with output_path.open("w", encoding="utf-8") as output_file:
        proc = subprocess.Popen(
            _raydp_submit_command(ray_conf_path, driver_script_path),
            cwd=str(_repo_root()),
            env=_subprocess_env("killed"),
            text=True,
            stdout=output_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        # The driver sleeps for 300s after the Spark action; waiting here keeps
        # the test simple while still killing raydp-submit during driver runtime.
        time.sleep(KILL_AFTER_SECONDS)
        log = output_path.read_text(encoding="utf-8")
        assert proc.poll() is None, log
        assert KILLED_RUNNING_SENTINEL in log, (
            f"Driver never reached the killed branch; stdout so far:\n{log}"
        )

        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10)

    assert proc.returncode != 0, output_path.read_text(encoding="utf-8")
