import re
import shlex
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

import anyio
import googleapiclient
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from googleapiclient.discovery import Resource
from prefect.logging.loggers import PrefectLogAdapter
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from prefect.utilities.dockerutils import get_prefect_image_name
from prefect.utilities.pydantic import JsonPatch
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import Field, validator

from prefect_gcp.cloud_run_v2 import ExecutionV2, JobV2
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


class CloudRunV2WorkerVariables(BaseVariables):
    """
    Default variables for the Cloud Run v2 worker.

    The schema for this class is used to populate the `variables` section of the
    default base job template.
    """

    region: str = Field(
        default="us-central1",
        description="The region where the Cloud Run Job resides.",
        example="us-central1",
    )
    credentials: Optional[GcpCredentials] = Field(
        title="GCP Credentials",
        default_factory=GcpCredentials,
        description="The GCP Credentials used to initiate the "
        "Cloud Run Job. If not provided credentials will be "
        "inferred from the local environment.",
    )
    image: Optional[str] = Field(
        default=None,
        title="Image Name",
        description=(
            "The image to use for a new Cloud Run Job. "
            "If not set, the latest Prefect image will be used. "
            "See https://cloud.google.com/run/docs/deploying#images."
        ),
        example="docker.io/prefecthq/prefect:2-latest",
    )
    cpu: Optional[str] = Field(
        default=None,
        title="CPU",
        description=(
            "The amount of compute allocated to the Cloud Run Job. "
            "(1000m = 1 CPU). See "
            "https://cloud.google.com/run/docs/configuring/cpu#setting-jobs."
        ),
        example="1000m",
        regex=r"^(\d*000)m$",
    )
    memory: Optional[str] = Field(
        default=None,
        title="Memory",
        description=(
            "The amount of memory allocated to the Cloud Run Job. "
            "Must be specified in units of 'G', 'Gi', 'M', or 'Mi'. "
            "See https://cloud.google.com/run/docs/configuring/memory-limits#setting."
        ),
        example="512Mi",
        regex=r"^\d+(?:G|Gi|M|Mi)$",
    )
    vpc_connector_name: Optional[str] = Field(
        default=None,
        title="VPC Connector Name",
        description="The name of the VPC connector to use for the Cloud Run Job.",
    )
    service_account: Optional[str] = Field(
        default=None,
        title="Service Account",
        description="The name of the service account to use for the task execution "
        "of Cloud Run Job. By default Cloud Run jobs run as the default "
        "Compute Engine Service Account. ",
        example="service-account@example.iam.gserviceaccount.com",
    )
    keep_job: Optional[bool] = Field(
        default=False,
        title="Keep Job After Completion",
        description="Keep the completed Cloud Run Job after it has run.",
    )
    timeout: Optional[int] = Field(
        default=600,
        gt=0,
        le=8400,
        title="Job Timeout",
        description=(
            "The length of time that Prefect will wait for Cloud Run Job state changes."
        ),
    )


class CloudRunV2WorkerResult(BaseWorkerResult):
    """
    Contains information about the final state of a completed process
    """


class CloudRunV2Worker(BaseWorker):
    """
    Prefect worker that executes flow runs within Cloud Run Jobs.
    """

    type = "cloud-run-v2"
    job_configuration = CloudRunV2WorkerJobConfiguration
    job_configuration_variables = CloudRunV2WorkerVariables
    _description = (
        "Execute flow runs within containers on Google Cloud Run v2. Requires "
        "a Google Cloud Platform account."
    )
    _display_name = "Google Cloud Run v2"
    _documentation_url = "https://prefecthq.github.io/prefect-gcp/worker_v2/"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/4SpnOBvMYkHp6z939MDKP6/549a91bc1ce9afd4fb12c68db7b68106/social-icon-google-cloud-1200-630.png?h=250"  # noqa

    def _create_job_error(self, exc, configuration):
        """Provides a nicer error for 404s when trying to create a Cloud Run Job."""
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

    async def run(
        self,
        flow_run: "FlowRun",
        configuration: CloudRunV2WorkerJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> CloudRunV2WorkerResult:
        """
        Executes a flow run within a Cloud Run v2 Job and waits for the flow run
        to complete.

        Args:
            flow_run: The flow run to execute
            configuration: The configuration to use when executing the flow run.
            task_status: The task status object for the current flow run. If provided,
                the task will be marked as started.

        Returns:
            CloudRunWorkerResult: A result object containing information about the
                final state of the flow run
        """

        logger = self.get_flow_run_logger(flow_run)

        with self._get_client(configuration) as client:
            await run_sync_in_worker_thread(
                self._create_job_and_wait_for_registration,
                configuration,
                client,
                logger,
            )
            job_execution = await run_sync_in_worker_thread(
                self._begin_job_execution, configuration, client, logger
            )

            if task_status:
                task_status.started(configuration.job_name)

            result = await run_sync_in_worker_thread(
                self._watch_job_execution_and_get_result,
                configuration,
                client,
                job_execution,
                logger,
            )
            return result

    @staticmethod
    def _get_client(configuration: CloudRunV2WorkerJobConfiguration) -> Resource:
        """
        Get the base client needed for interacting with GCP APIs.

        Returns:
            A client for interacting with GCP Cloud Run v2 API.
        """
        api_endpoint = f"https://{configuration.region}-run.googleapis.com"
        gcp_creds = configuration.credentials.get_credentials_from_service_account()
        options = ClientOptions(api_endpoint=api_endpoint)

        return (
            discovery.build(
                "run",
                "v2",
                client_options=options,
                credentials=gcp_creds,
            )
            .projects()
            .locations()
        )

    def _create_job_and_wait_for_registration(
        self,
        configuration: CloudRunV2WorkerJobConfiguration,
        client: Resource,
        logger: PrefectLogAdapter,
    ) -> None:
        """
        Create a new job wait for it to finish registering.
        """
        try:
            logger.info(f"Creating Cloud Run Job {configuration.job_name}")

            JobV2.create(
                client=client,
                project=configuration.credentials.project,
                location=configuration.region,
                body=configuration.job_body,
            )
        except googleapiclient.errors.HttpError as exc:
            self._create_job_error(exc, configuration)

        try:
            self._wait_for_job_creation(
                client=client, configuration=configuration, logger=logger
            )
        except Exception:
            logger.exception(
                "Encountered an exception while waiting for job run creation"
            )
            if not configuration.keep_job:
                logger.info(
                    f"Deleting Cloud Run Job {configuration.job_name} from "
                    "Google Cloud Run."
                )
                try:
                    JobV2.delete(
                        client=client,
                        project=configuration.credentials.project,
                        location=configuration.region,
                        job_name=configuration.job_name,
                    )
                except Exception:
                    logger.exception(
                        "Received an unexpected exception while attempting to delete"
                        f" Cloud Run Job {configuration.job_name!r}"
                    )
            raise

    def _begin_job_execution(
        self,
        configuration: CloudRunV2WorkerJobConfiguration,
        client: Resource,
        logger: PrefectLogAdapter,
    ) -> ExecutionV2:
        """
        Submit a job run for execution and return the execution object.
        """
        try:
            logger.info(
                f"Submitting Cloud Run Job {configuration.job_name!r} for execution."
            )
            submission = JobV2.run(
                client=client,
                project=configuration.credentials.project,
                location=configuration.region,
                job_name=configuration.job_name,
            )

            job_execution = ExecutionV2.get(
                client=client,
                execution_name=submission["metadata"]["name"],
            )

            return job_execution
        except Exception as exc:
            self._job_run_submission_error(exc, configuration)

    def _watch_job_execution_and_get_result(
        self,
        configuration: CloudRunV2WorkerJobConfiguration,
        client: Resource,
        execution: ExecutionV2,
        logger: PrefectLogAdapter,
        poll_interval: int = 5,
    ) -> CloudRunV2WorkerResult:
        """Wait for execution to complete and then return result."""
        try:
            job_execution = self._watch_job_execution(
                client=client,
                job_execution=execution,
                timeout=configuration.timeout,
                poll_interval=poll_interval,
            )
        except Exception:
            logger.exception(
                "Received an unexpected exception while monitoring Cloud Run Job "
                f"{configuration.job_name!r}"
            )
            raise

        if job_execution.succeeded():
            status_code = 0
            logger.info(f"Job Run {configuration.job_name} completed successfully")
        else:
            status_code = 1
            error_msg = job_execution.condition_after_completion()["message"]
            logger.error(
                "Job Run {configuration.job_name} did not complete successfully. "
                f"{error_msg}"
            )

        logger.info(f"Job Run logs can be found on GCP at: {job_execution.log_uri}")

        if not configuration.keep_job:
            logger.info(
                f"Deleting completed Cloud Run Job {configuration.job_name!r} "
                "from Google Cloud Run..."
            )
            try:
                JobV2.delete(
                    client=client,
                    namespace=configuration.project,
                    job_name=configuration.job_name,
                )
            except Exception:
                logger.exception(
                    "Received an unexpected exception while attempting to delete Cloud"
                    f" Run Job {configuration.job_name}"
                )

        return CloudRunV2WorkerResult(
            identifier=configuration.job_name,
            status_code=status_code,
        )

    @staticmethod
    def _watch_job_execution(
        client,
        job_execution: ExecutionV2,
        timeout: int,
        poll_interval: int = 5,
    ):
        """
        Update job_execution status until it is no longer running or timeout is reached.
        """
        t0 = time.time()
        while job_execution.is_running():
            job_execution = ExecutionV2.get(
                client=client,
                execution_name=job_execution.name,
            )

            elapsed_time = time.time() - t0
            if timeout is not None and elapsed_time > timeout:
                raise RuntimeError(
                    f"Timed out after {elapsed_time}s while waiting for Cloud Run Job "
                    "execution to complete. Your job may still be running on GCP."
                )

            time.sleep(poll_interval)

        return job_execution

    @staticmethod
    def _wait_for_job_creation(
        client: Resource,
        configuration: CloudRunV2WorkerJobConfiguration,
        logger: PrefectLogAdapter,
        poll_interval: int = 5,
    ):
        """Give created job time to register."""
        job = JobV2.get(
            client=client,
            project=configuration.project,
            location=configuration.region,
            job_name=configuration.job_name,
        )

        t0 = time.time()
        while not job.is_ready():
            ready_condition = (
                job.ready_condition
                if job.ready_condition
                else "waiting for condition update"
            )
            logger.info(f"Job is not yet ready... Current condition: {ready_condition}")
            job = JobV2.get(
                client=client,
                project=configuration.project,
                location=configuration.region,
                job_name=configuration.job_name,
            )

            elapsed_time = time.time() - t0
            if (
                configuration.timeout is not None
                and elapsed_time > configuration.timeout
            ):
                raise RuntimeError(
                    f"Timed out after {elapsed_time}s while waiting for Cloud Run Job "
                    "execution to complete. Your job may still be running on GCP."
                )

            time.sleep(poll_interval)

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: CloudRunV2WorkerJobConfiguration,
        grace_seconds: int = 30,
    ):
        """
        Stops a job for a cancelled flow run based on the provided infrastructure PID
        and run configuration.
        """
        if grace_seconds != 30:
            self._logger.warning(
                f"Kill grace period of {grace_seconds}s requested, but GCP does not "
                "support dynamic grace period configuration. See here for more info: "
                "https://cloud.google.com/run/docs/reference/rest/v1/namespaces.jobs/delete"  # noqa
            )

        with self._get_client(configuration) as client:
            await run_sync_in_worker_thread(
                self._stop_job,
                client=client,
                namespace=configuration.project,
                job_name=infrastructure_pid,
            )

    # @staticmethod
    # def _stop_job(
    #     client: Resource,
    #     project: str,
    #     location: str,
    #     job_name: str,
    # ):
    #     try:
    #         JobV2.delete(
    #             client=client,
    #             project=project,
    #             location=location,
    #             job_name=job_name,
    #         )
    #     except Exception as exc:
    #         if "does not exist" in str(exc):
    #             raise InfrastructureNotFound(
    #                 f"Cannot stop Cloud Run Job; the job name {job_name!r} "
    #                 "could not be found."
    #             ) from exc
    #         raise
