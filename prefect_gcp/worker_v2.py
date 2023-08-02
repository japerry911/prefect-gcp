import shlex
from typing import TYPE_CHECKING, Any, Dict, Optional

from prefect.utilities.dockerutils import get_prefect_image_name
from prefect.utilities.pydantic import JsonPatch
from prefect.workers.base import BaseJobConfiguration
from pydantic import Field, validator

from prefect_gcp.credentials import GcpCredentials

if TYPE_CHECKING:
    from prefect.client.schemas import FlowRun
    from prefect.server.schemas.core import Flow
    from prefect.server.schemas.responses import DeploymentResponse


def _get_default_job_body_template() -> Dict[str, Any]:
    """
    Returns the default job body template used by the Cloud Run Job.
    """
    return {
        "name": "{{ name }}",
        "client": "{{ client }}",
        "client_version": "{{ client_version }}",
        "launch_stage": "{{ launch_stage }}",
        "binary_authorization": "{{ binary_authorization }}",
        "template": {
            "parallelism": "{{ parallelism }}",
            "task_count": "{{ task_count }}",
            "template": {
                "containers": [
                    {
                        "image": "{{ image }}",
                        "command": "{{ command }}",
                        "resources": {
                            "limits": {
                                "cpu": "{{ cpu }}",
                                "memory": "{{ memory }}",
                            }
                        },
                    }
                ],
                "timeout": "{{ timeout }}",
                "service_account": "{{ service_account }}",
                "execution_environment": "{{ execution_environment }}",
                "vpc_access": "{{ vpc_access }}",
                "max_retries": "{{ max_retries }}",
            },
        },
    }


def _get_base_job_body() -> Dict[str, Any]:
    """
    Returns a base job body to use for job body validation.
    """
    return {
        "name": "{{ name }}",
        "client": "{{ client }}",
        "client_version": "{{ client_version }}",
        "launch_stage": "{{ launch_stage }}",
        "binary_authorization": "{{ binary_authorization }}",
        "template": {
            "parallelism": "{{ parallelism }}",
            "task_count": "{{ task_count }}",
            "template": {"containers": [{}]},
        },
    }


class CloudRunV2WorkerJobConfiguration(BaseJobConfiguration):
    """ToDo"""

    region: str = Field(
        default="us-central1",
        description="The region where the Cloud Run Job resides",
    )
    credentials: Optional[GcpCredentials] = Field(
        title="GCP Credentials",
        default_factory=GcpCredentials,
        description="The GCP Credentials used to connect to Cloud Run. "
        "If not provided credentials will be inferred from "
        "the local environment",
    )
    job_body: Dict[str, Any] = Field(template=_get_default_job_body_template())
    timeout: Optional[int] = Field(
        default=600,
        gt=0,
        le=86400,
        title="Job Timeout",
        description=(
            "The length of time that Prefect will wait for a Cloud Run Job to complete "
            "before raising an exception."
        ),
    )
    keep_job: Optional[bool] = Field(
        default=False,
        title="Keep Job After Completion",
        description="Keep the completed Cloud Run Job on Google Cloud Platform.",
    )

    @property
    def project(self) -> str:
        """property for accessing the project from the credentials."""
        return self.credentials.project

    @property
    def job_name(self) -> str:
        """property for accessing the name from the job metadata."""
        return self.job_body["name"]

    def prepare_for_flow_run(
        self,
        flow_run: "FlowRun",
        deployment: Optional["DeploymentResponse"] = None,
        flow: Optional["Flow"] = None,
    ):
        """
        Prepares the job configuration for a flow run.

        Ensures that necessary values are present in the job body and that the
        job body is valid.

        Args:
            flow_run: The flow run to prepare the job configuration for
            deployment: The deployment associated with the flow run used for
                preparation.
            flow: The flow associated with the flow run used for preparation.
        """
        super().prepare_for_flow_run(
            flow_run=flow_run,
            deployment=deployment,
            flow=flow,
        )

        self._populate_envs()
        self._populate_or_format_command()
        self._format_args_if_present()
        self._populate_image_if_not_present()
        self._populate_name_if_not_present()

    def _populate_envs(self):
        """
        Populate environment variables. BaseWorker.prepare_for_flow_run handles
        putting the environment variables in the `env` attribute. This method
        moves them into the jobs body
        """
        envs = [{"name": k, "value": v} for k, v in self.env.items()]
        self.job_body["template"]["template"]["containers"][0]["env"] = envs

    def _populate_name_if_not_present(self):
        """
        Adds the flow run name to the job if one is not already provided.
        """
        try:
            if "name" not in self.job_body:
                self.job_body["name"] = self.name
        except KeyError:
            raise ValueError("Unable to verify name due to invalid job body template.")

    def _populate_image_if_not_present(self):
        """
        Adds the latest prefect image to the job if one is not already provided.
        """
        try:
            if "image" not in self.job_body["template"]["template"]["containers"][0]:
                self.job_body["template"]["template"]["containers"][0][
                    "image"
                ] = f"docker.io/{get_prefect_image_name()}"
        except KeyError:
            raise ValueError("Unable to verify image due to invalid job body template.")

    def _populate_or_format_command(self):
        """
        Ensures that the command is present in the job manifest. Populates the command
        with the `prefect -m prefect.engine` if a command is not present.
        """
        try:
            command = self.job_body["template"]["template"]["containers"][0].get(
                "command"
            )
            if command is None:
                self.job_body["template"]["template"]["containers"][0]["command"] = [
                    "python",
                    "-m",
                    "prefect.engine",
                ]
            elif isinstance(command, str):
                self.job_body["template"]["template"]["containers"][0][
                    "command"
                ] = shlex.split(command)
        except KeyError:
            raise ValueError(
                "Unable to verify command due to invalid job body template."
            )

    def _format_args_if_present(self):
        try:
            args = self.job_body["template"]["template"]["containers"][0].get("args")
            if args is not None and isinstance(args, str):
                self.job_body["template"]["template"]["containers"][0][
                    "args"
                ] = shlex.split(args)
        except KeyError:
            raise ValueError("Unable to verify args due to invalid job body template.")

    @validator("job_body")
    def _ensure_job_includes_all_required_components(cls, value: Dict[str, Any]):
        """
        Ensures that the job body includes all required components.
        """
        patch = JsonPatch.from_diff(value, _get_base_job_body())
        missing_paths = sorted([op["path"] for op in patch if op["op"] == "add"])
        if missing_paths:
            raise ValueError(
                "Job is missing required attributes at the following paths: "
                f"{', '.join(missing_paths)}"
            )
        return value

    @validator("job_body")
    def _ensure_job_has_compatible_values(cls, value: Dict[str, Any]):
        """
        Ensure that the job body has compatible values.
        """
        patch = JsonPatch.from_diff(value, _get_base_job_body())
        incompatible = sorted(
            [
                f"{op['path']} must have value {op['value']!r}"
                for op in patch
                if op["op"] == "replace"
            ]
        )
        if incompatible:
            raise ValueError(
                "Job has incompatible values for the following attributes: "
                f"{', '.join(incompatible)}"
            )
        return value
