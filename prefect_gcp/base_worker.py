from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Optional

from prefect.workers.base import BaseJobConfiguration, BaseVariables
from pydantic import validator

if TYPE_CHECKING:
    from prefect.client.schemas import FlowRun
    from prefect.server.schemas.core import Flow
    from prefect.server.schemas.responses import DeploymentResponse

from prefect_gcp.credentials import GcpCredentials


class BaseCloudRunWorkerJobConfiguration(BaseJobConfiguration, ABC):
    """ToDo"""

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

    @abstractmethod
    def _populate_envs(self):
        """
        Populate environment variables. BaseWorker.prepare_for_flow_run handles
        putting the environment variables in the `env` attribute. This method
        moves them into the jobs body
        """

    @abstractmethod
    def _populate_name_if_not_present(self):
        """
        Adds the flow run name to the job if one is not already provided.
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
        """ToDo"""

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
        Ensure that the job body has compatible values.
        """


class BaseCloudRunWorkerVariables(BaseVariables, ABC):
    """ToDo"""

    region: str
    credentials: Optional[GcpCredentials]
    image: Optional[str]
    cpu: Optional[str]
    memory: Optional[str]
    vpc_connector_name: Optional[str]
    # ToDo: start from here
