import uuid

import pydantic
import pytest
from prefect.flows import FlowRun

from prefect_gcp.credentials import GcpCredentials
from prefect_gcp.worker_v2 import CloudRunV2Worker, CloudRunV2WorkerJobConfiguration


@pytest.fixture
def jobs_body():
    return {
        "template": {
            "template": {"containers": [{}]},
        },
    }


@pytest.fixture
def flow_run():
    return FlowRun(flow_id=uuid.uuid4(), name="my-flow-run-name")


@pytest.fixture
def cloud_run_v2_worker_job_config(service_account_info, jobs_body):
    return CloudRunV2WorkerJobConfiguration(
        name="my-job-name",
        image="gcr.io//not-a-real-image",
        region="pluto",
        job_body=jobs_body,
        credentials=GcpCredentials(service_account_info=service_account_info),
    )


class TestCloudRunV2WorkerJobConfiguration:
    def test_job_name(self, cloud_run_v2_worker_job_config):
        cloud_run_v2_worker_job_config.job_body["name"] = "my-job-name"
        assert cloud_run_v2_worker_job_config.job_name == "my-job-name"

    def test_populate_envs(self, cloud_run_v2_worker_job_config):
        container = cloud_run_v2_worker_job_config.job_body["template"]["template"][
            "containers"
        ][0]

        assert "env" not in container
        assert cloud_run_v2_worker_job_config.env == {}
        cloud_run_v2_worker_job_config.env = {"a": "b"}

        cloud_run_v2_worker_job_config._populate_envs()

        assert "env" in container
        assert len(container["env"]) == 1
        assert container["env"][0]["name"] == "a"
        assert container["env"][0]["value"] == "b"

    def test_populate_image_if_not_present(self, cloud_run_v2_worker_job_config):
        container = cloud_run_v2_worker_job_config.job_body["template"]["template"][
            "containers"
        ][0]

        assert "image" not in container

        cloud_run_v2_worker_job_config._populate_image_if_not_present()

        # defaults to prefect image
        assert "image" in container
        assert container["image"].startswith("docker.io/prefecthq/prefect:")

    def test_populate_image_doesnt_overwrite(self, cloud_run_v2_worker_job_config):
        image = "my-first-image"
        container = cloud_run_v2_worker_job_config.job_body["template"]["template"][
            "containers"
        ][0]
        container["image"] = image

        cloud_run_v2_worker_job_config._populate_image_if_not_present()

        assert container["image"] == image

    def test_populate_or_format_command_doesnt_exist(
        self,
        cloud_run_v2_worker_job_config,
    ):
        container = cloud_run_v2_worker_job_config.job_body["template"]["template"][
            "containers"
        ][0]

        assert "command" not in container

        cloud_run_v2_worker_job_config._populate_or_format_command()

        assert "command" in container
        assert container["command"] == ["python", "-m", "prefect.engine"]

    def test_populate_or_format_command_already_exists(
        self,
        cloud_run_v2_worker_job_config,
    ):
        command = "my command and args"
        container = cloud_run_v2_worker_job_config.job_body["template"]["template"][
            "containers"
        ][0]
        container["command"] = command

        cloud_run_v2_worker_job_config._populate_or_format_command()

        assert "command" in container
        assert container["command"] == command.split()

    def test_format_args_if_present(self, cloud_run_v2_worker_job_config):
        args = "my args"
        container = cloud_run_v2_worker_job_config.job_body["template"]["template"][
            "containers"
        ][0]
        container["args"] = args

        cloud_run_v2_worker_job_config._format_args_if_present()

        assert "args" in container
        assert container["args"] == args.split()

    def test_format_args_if_present_no_args(self, cloud_run_v2_worker_job_config):
        container = cloud_run_v2_worker_job_config.job_body["template"]["template"][
            "containers"
        ][0]
        assert "args" not in container

        cloud_run_v2_worker_job_config._format_args_if_present()

        assert "args" not in container

    async def test_validates_against_an_empty_job_body(self):
        template = CloudRunV2Worker.get_default_base_job_template()
        template["job_configuration"]["job_body"] = {}
        template["job_configuration"]["region"] = "test-region1"

        with pytest.raises(pydantic.ValidationError) as excinfo:
            await CloudRunV2WorkerJobConfiguration.from_template_and_values(
                template,
                {},
            )

        assert excinfo.value.errors() == [
            {
                "loc": ("job_body",),
                "msg": (
                    "Job is missing required attributes at the following paths: "
                    "/template"
                ),
                "type": "value_error",
            }
        ]

    async def test_validates_for_a_job_body_missing_deeper_attributes(self):
        template = CloudRunV2Worker.get_default_base_job_template()
        template["job_configuration"]["job_body"] = {
            "template": {
                "template": {},
            },
        }
        template["job_configuration"]["region"] = "test-region1"

        with pytest.raises(pydantic.ValidationError) as excinfo:
            await CloudRunV2WorkerJobConfiguration.from_template_and_values(
                template,
                {},
            )

        assert excinfo.value.errors() == [
            {
                "loc": ("job_body",),
                "msg": (
                    "Job is missing required attributes at the following paths: "
                    "/template/template/containers"
                ),
                "type": "value_error",
            }
        ]


class TestCloudRunV2WorkerValidConfiguration:
    """ToDo"""
