import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Optional

from googleapiclient.discovery import Resource
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import validator

if TYPE_CHECKING:
    from prefect.client.schemas import FlowRun
    from prefect.server.schemas.core import Flow
    from prefect.server.schemas.responses import DeploymentResponse

from prefect_gcp.credentials import GcpCredentials


class BaseCloudRunWorkerJobConfiguration(BaseJobConfiguration, ABC):
    """
    Base Configuration class used by the Cloud Run Worker (v1 and v2) to create a
    Cloud Run Job.
    """

    region: str
    credentials: Optional[GcpCredentials]
    job_body: Dict[str, Any]
    timeout: Optional[int]
    keep_job: Optional[bool]

    @property
    def project(self) -> str:
        """
        Property for accessing the project from the credentials.
        """
        return self.credentials.project

    @property
    @abstractmethod
    def job_name(self) -> str:
        """
        Property for accessing the name from the job metadata.
        """

    @abstractmethod
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
        """
        super().prepare_for_flow_run(
            flow_run=flow_run,
            deployment=deployment,
            flow=flow,
        )

    @abstractmethod
    def _populate_envs(self):
        """
        Populate environment variables. BaseWorker.prepare_for_flow_run handles
        putting the environment variables in the `env` attribute. This method
        moves them into the jobs body
        """

    @abstractmethod
    def _populate_image_if_not_present(self):
        """
        Adds the latest prefect image to the job if one is not already provided.
        """

    @abstractmethod
    def _populate_or_format_command(self):
        """
        Ensures that the command is present in the job manifest. Populates the command
        with the `prefect -m prefect.engine` if a command is not present.
        """

    @abstractmethod
    def _format_args_if_present(self):
        """
        Formats the arguments if they are present.
        """

    @validator("job_body")
    @abstractmethod
    def _ensure_job_includes_all_required_components(cls, value: Dict[str, Any]):
        """
        Ensures that the job body includes all required components.
        """

    @validator("job_body")
    @abstractmethod
    def _ensure_job_has_compatible_values(cls, value: Dict[str, Any]):
        """
        Ensures that the job body has compatible values.
        """


class BaseCloudRunWorkerVariables(BaseVariables, ABC):
    """
    Base Default variables for the Cloud Run worker.
    """

    region: str
    credentials: Optional[GcpCredentials]
    image: Optional[str]
    cpu: Optional[str]
    memory: Optional[str]
    vpc_connector_name: Optional[str]
    service_account_name: Optional[str]
    keep_job: Optional[bool]
    timeout: Optional[int]


class BaseCloudRunWorkerResult(BaseWorkerResult, ABC):
    """
    Contains information about the final state of a completed process
    """


class BaseCloudRunWorker(BaseWorker):
    """
    Prefect worker that executes flow runs within Cloud Run Jobs.
    """

    type = "base-cloud-run"
    _description = "Base Cloud Run Worker used in v1 and v2 Cloud Run Workers."
    _display_name = "Base Google Cloud Run"
    _documentation_url = "https://prefecthq.github.io/prefect-gcp/base_worker/"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4SpnOBvMYkHp6z939MDKP6/549a91bc1ce9afd4fb12c68db7b68106/social-icon-google-cloud-1200-630.png?h=250"  # noqa

    def _create_job_error(self, exc, configuration):
        """
        Provides a nicer error for 404s when trying to create a Cloud Run Job.
        """
        # TODO consider lookup table instead of the if/else,
        # also check for documented errors
        if exc.status_code == 404:
            raise RuntimeError(
                f"Failed to find resources at {exc.uri}. Confirm that region"
                f" '{self.region}' is the correct region for your Cloud Run Job and"
                f" that {configuration.project} is the correct GCP project. If"
                f" your project ID is not correct, you are using a Credentials block"
                f" with permissions for the wrong project."
            ) from exc
        raise exc

    def _job_run_submission_error(self, exc, configuration):
        """Provides a nicer error for 404s when submitting job runs."""
        if exc.status_code == 404:
            pat1 = r"The requested URL [^ ]+ was not found on this server"
            # pat2 = (
            #     r"Resource '[^ ]+' of kind 'JOB' in region '[\w\-0-9]+' "
            #     r"in project '[\w\-0-9]+' does not exist"
            # )
            if re.findall(pat1, str(exc)):
                raise RuntimeError(
                    f"Failed to find resources at {exc.uri}. "
                    f"Confirm that region '{self.region}' is "
                    f"the correct region for your Cloud Run Job "
                    f"and that '{configuration.project}' is the "
                    f"correct GCP project. If your project ID is not "
                    f"correct, you are using a Credentials "
                    f"block with permissions for the wrong project."
                ) from exc
            else:
                raise exc

        raise exc

    @abstractmethod
    async def run(self, *args, **kwargs):
        """
        Executes a flow run within a Cloud Run Job and waits for the flow run
        to complete.
        """

    @abstractmethod
    def _get_client(self, *args, **kwargs) -> Resource:
        """
        Get the base client needed for interacting with GCP APIs.
        """

    @abstractmethod
    def _create_job_and_wait_for_registration(self, *args, **kwargs) -> None:
        """
        Create a new job wait for it to finish registering.
        """

    @abstractmethod
    def _begin_job_execution(self, *args, **kwargs):
        """
        Submit a job run for execution and return the execution object.
        """

    @abstractmethod
    def _watch_job_execution_and_get_result(self, *args, **kwargs):
        """
        Wait for execution to complete and then return result.
        """

    @abstractmethod
    def _watch_job_execution(self, *args, **kwargs):
        """
        Update job_execution status until it is no longer running or timeout is reached.
        """

    @abstractmethod
    def _wait_for_job_creation(self, *args, **kwargs):
        """
        Give created job time to register.
        """

    @abstractmethod
    async def kill_infrastructure(self, *args, **kwargs):
        """
        Stops a job for a cancelled flow run based on the provided infrastructure PID
        and run configuration.
        """

    @abstractmethod
    def _stop_job(self, *args, **kwargs):
        """
        Stops/Deletes a Cloud Run job.
        """
