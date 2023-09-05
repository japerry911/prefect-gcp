import shlex
import time
from typing import TYPE_CHECKING, Any, Dict, Optional
from uuid import uuid4

import anyio
import googleapiclient
from google.api_core.client_options import ClientOptions
from googleapiclient import discovery
from googleapiclient.discovery import Resource
from prefect.exceptions import InfrastructureNotFound
from prefect.logging.loggers import PrefectLogAdapter
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from prefect.utilities.dockerutils import get_prefect_image_name
from prefect.utilities.pydantic import JsonPatch
from pydantic import Field, validator
from typing_extensions import Literal

from prefect_gcp.base_worker import (
    BaseCloudRunWorker,
    BaseCloudRunWorkerJobConfiguration,
    BaseCloudRunWorkerResult,
    BaseCloudRunWorkerVariables,
)
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
                "vpc_access": "{{ vpc_connector_name }}",
                "max_retries": "{{ max_retries }}",
            },
        },
    }


def _get_base_job_body() -> Dict[str, Any]:
    """
    Returns a base job body to use for job body validation.
    """
    return {
        "template": {
            "template": {"containers": [{}]},
        },
    }


class CloudRunV2WorkerJobConfiguration(BaseCloudRunWorkerJobConfiguration):
    """
    Base Configuration class used by the Cloud Run Worker (v2) to create a
    Cloud Run Job.

    An instance of this class is passed to the Cloud Run worker's `run` method
    for each flow run. It contains all information necessary to execute
    the flow run as a Cloud Run Job.

    Attributes:
        region: The region where the Cloud Run Job resides.
        credentials: The GCP Credentials used to connect to Cloud Run.
        job_body: The job body used to create the Cloud Run Job.
        timeout: The length of time that Prefect will wait for a Cloud Run Job.
        keep_job: Whether to delete the Cloud Run Job after it completes.
    """

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
    def job_name(self) -> str:
        """
        Property for accessing the name from the job body.

        Returns:
            A string, which is the job's name.
        """
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
        self._prepare_timeout()

    def _prepare_timeout(self):
        """
        Prepares timeout value to be in correct format (example: 900 => "900s")
        """
        timeout_seconds = f"{self.timeout}s"
        self.job_body["template"]["template"]["timeout"] = timeout_seconds

    def _populate_envs(self):
        """
        Populate environment variables. BaseWorker.prepare_for_flow_run handles
        putting the environment variables in the `env` attribute. This method
        moves them into the jobs body
        """
        envs = [{"name": k, "value": v} for k, v in self.env.items()]
        self.job_body["template"]["template"]["containers"][0]["env"] = envs

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
        """
        Formats the arguments if they are present.
        """
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

        Args:
            value: the Job body that is being checked.
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

        Args:
            value: The Job body being checked.
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


class CloudRunV2WorkerVariables(BaseCloudRunWorkerVariables):
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
    max_retries: Optional[int] = Field(
        default=1,
        title="Max Retries",
        description=(
            "Number of retries allowed per Task, before marking this Task failed."
        ),
    )
    launch_stage: Optional[Literal["ALPHA", "BETA", "GA"]] = Field(
        default="BETA",  # ToDo: Should this be GA, or BETA?
        title="Launch Stage",
        description=(
            "The launch stage as defined by Google Cloud Platform Launch Stages."
            "https://cloud.google.com/run/docs/reference/rest/v2/LaunchStage."
        ),
    )
    vpc_connector_name: Optional[str] = Field(
        default=None,
        title="VPC Connector Name",
        description="The name of the VPC connector to use for the Cloud Run Job.",
    )
    service_account: Optional[str] = Field(
        default=None,
        title="Service Account Name",
        description="The name of the service account to use for the task execution "
        "of Cloud Run Job. By default Cloud Run jobs run as the default "
        "Compute Engine Service Account. ",
        example="service-account@example.iam.gserviceaccount.com",
    )
    binary_authorization: Optional[Dict] = Field(
        default={},
        title="Binary Authorization",
        description=(
            "Settings for Binary Authorization feature."
            "https://cloud.google.com/run/docs/reference/rest/v2/BinaryAuthorization."
        ),
    )
    parallelism: Optional[int] = Field(
        default=1,
        title="Parallelism",
        description=(
            "Specifies the maximum desired number of tasks the execution should "
            "run at given time. Must be <= taskCount. When the job is run, if "
            "this field is 0 or unset, the maximum possible value will be used for "
            "that execution. The actual number of tasks running in steady state "
            "will be less than this number when there are fewer tasks waiting to "
            "be completed remaining, i.e. when the work left to do is less than "
            "max parallelism."
        ),
    )
    task_count: Optional[int] = Field(
        default=1,
        title="Task Count",
        description=(
            "Specifies the desired number of tasks the execution should run. "
            "Setting to 1 means that parallelism is limited to 1 and the success "
            "of that task signals the success of the execution. Defaults to 1."
        ),
    )
    keep_job: Optional[bool] = Field(
        default=False,
        title="Keep Job After Completion",
        description="Keep the completed Cloud Run Job after it has run.",
    )
    timeout: Optional[int] = Field(
        default=600,
        gt=0,
        le=86400,
        title="Job Timeout",
        description=(
            "The length of time that Prefect will wait for Cloud Run Job state changes."
        ),
    )


class CloudRunV2WorkerResult(BaseCloudRunWorkerResult):
    """
    Contains information about the final state of a completed process
    """


class CloudRunV2Worker(BaseCloudRunWorker):
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

    @staticmethod
    def _create_job_id(configuration: CloudRunV2WorkerJobConfiguration) -> str:
        """
        Creates a Job ID based of image, if that is available, otherwise it generates a
            random one.

        Args:
            configuration: The Cloud Run v2 Worker Job Configuration, which will
                hold image, if it exists.
        Returns:
            The newly created Job ID.
        """
        image = (
            configuration.job_body.get("template")
            .get("template")
            .get("containers")[0]
            .get("image")
        )

        if image is not None:
            # get `repo` from `gcr.io/<project_name>/repo/other`
            components = image.split("/")
            image_name = components[2]
            # only alphanumeric and '-' allowed for a job name
            modified_image_name = image_name.replace(":", "-").replace(".", "-")
            # make 50 char limit for final job name, which will be '<name>-<uuid>'
            if len(modified_image_name) > 17:
                modified_image_name = modified_image_name[:17]
            job_id = f"{modified_image_name}-{uuid4().hex}"
        else:
            job_id = f"prefect-{uuid4().hex}"

        return job_id

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
            job_id = await run_sync_in_worker_thread(
                self._create_job_and_wait_for_registration,
                configuration,
                client,
                logger,
            )
            job_execution = await run_sync_in_worker_thread(
                self._begin_job_execution,
                configuration,
                client,
                logger,
                job_id,
            )

            if task_status:
                task_status.started(job_id)

            result = await run_sync_in_worker_thread(
                self._watch_job_execution_and_get_result,
                configuration,
                client,
                job_execution,
                logger,
                job_id,
            )
            return result

    @staticmethod
    def _get_client(configuration: CloudRunV2WorkerJobConfiguration) -> Resource:
        """
        Get the base client needed for interacting with GCP APIs.

        Args:
            configuration: The Cloud Run v2 Worker Job Configuration.
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
    ) -> str:
        """
        Create a new job wait for it to finish registering.

        Args:
            configuration: THe Cloud Run v2 Worker Job Configuration.
            client: The googleapiclient.discovery Resource for Cloud Run v2 API.
            logger: The Prefect Logger.
        Returns:
            The Job ID that is created in conjunction with the creation of the Job.
        """
        job_id = self._create_job_id(configuration=configuration)

        try:
            logger.info(f"Creating Cloud Run Job {job_id}")

            JobV2.create(
                client=client,
                project=configuration.credentials.project,
                location=configuration.region,
                job_id=job_id,
                body=configuration.job_body,
            )

        except googleapiclient.errors.HttpError as exc:
            self._create_job_error(exc, configuration)

        try:
            self._wait_for_job_creation(
                client=client,
                configuration=configuration,
                logger=logger,
                job_id=job_id,
            )
        except Exception:
            logger.exception(
                "Encountered an exception while waiting for job run creation"
            )
            if not configuration.keep_job:
                logger.info(
                    f"Deleting Cloud Run Job {job_id} from " "Google Cloud Run."
                )
                try:
                    JobV2.delete(
                        client=client,
                        project=configuration.credentials.project,
                        location=configuration.region,
                        job_name=job_id,
                    )
                except Exception:
                    logger.exception(
                        "Received an unexpected exception while attempting to delete"
                        f" Cloud Run Job {job_id!r}"
                    )
            raise

        return job_id

    def _begin_job_execution(
        self,
        configuration: CloudRunV2WorkerJobConfiguration,
        client: Resource,
        logger: PrefectLogAdapter,
        job_id: str,
    ) -> ExecutionV2:
        """
        Submit a job run for execution and return the execution object.

        Args:
            configuration: THe Cloud Run v2 Worker Job Configuration.
            client: The googleapiclient.discovery Resource for Cloud Run v2 API.
            logger: The Prefect Logger.
            job_id: The Job ID for the Job that is beginning execution.
        Returns:
             The Job Execution object of the running Job.
        """
        try:
            logger.info(f"Submitting Cloud Run Job {job_id!r} for execution.")
            submission = JobV2.run(
                client=client,
                project=configuration.credentials.project,
                location=configuration.region,
                job_name=job_id,
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
        job_id: str,
        poll_interval: int = 5,
    ) -> CloudRunV2WorkerResult:
        """
        Wait for execution to complete and then return result.

        Args:
            configuration: THe Cloud Run v2 Worker Job Configuration.
            client: The googleapiclient.discovery Resource for Cloud Run v2 API.
            execution: The running Job's Execution object.
            logger: The Prefect Logger.
            job_id: The running Job's Job ID.
            poll_interval: The polling interval for watching the Job's Execution.
        Returns:
            The Cloud Run v2 Worker Result of the finished Job.
        """
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
                f"{job_id!r}"
            )
            raise

        if job_execution.succeeded():
            status_code = 0
            logger.info(f"Job Run {job_id} completed successfully")
        else:
            status_code = 1
            error_msg = job_execution.condition_after_completion().get("message")
            logger.error(f"Job Run {job_id} did not complete successfully. {error_msg}")

        logger.info(f"Job Run logs can be found on GCP at: {job_execution.log_uri}")

        if not configuration.keep_job:
            logger.info(
                f"Deleting completed Cloud Run Job {job_id!r} "
                "from Google Cloud Run..."
            )
            try:
                JobV2.delete(
                    client=client,
                    project=configuration.project,
                    location=configuration.region,
                    job_name=job_id,
                )
            except Exception:
                logger.exception(
                    "Received an unexpected exception while attempting to delete Cloud"
                    f" Run Job {job_id}"
                )

        return CloudRunV2WorkerResult(
            identifier=job_id,
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

        Args:
            client: The googleapiclient.discovery Resource for Cloud Run v2 API.
            job_execution: The Execution object of Job that is being watched.
            timeout: The total time that the Job has to complete before an exception
                is raised.
            poll_interval: The polling interval between status checks on the Job.
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

    def _wait_for_job_creation(
        self,
        client: Resource,
        configuration: CloudRunV2WorkerJobConfiguration,
        logger: PrefectLogAdapter,
        job_id: str,
        poll_interval: int = 5,
    ):
        """
        Give created job time to register.

        Args:
            configuration: THe Cloud Run v2 Worker Job Configuration.
            client: The googleapiclient.discovery Resource for Cloud Run v2 API.
            logger: The Prefect Logger.
            job_id: The Job ID of the Job being waiting to be created.
            poll_interval: The polling interval between checking the creation status.
        """
        job = JobV2.get(
            client=client,
            project=configuration.project,
            location=configuration.region,
            job_name=job_id,
        )

        t0 = time.time()
        while not job.is_ready():
            ready_condition = (
                job.get_ready_condition()
                if job.get_ready_condition()  # ToDo: add walrus operator?
                else "waiting for condition update"
            )
            logger.info(f"Job is not yet ready... Current condition: {ready_condition}")
            job = JobV2.get(
                client=client,
                project=configuration.project,
                location=configuration.region,
                job_name=job_id,
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

        Args:
            infrastructure_pid: The Infrastructure PID, which in this case is the
                Job's Name.
            configuration: THe Cloud Run v2 Worker Job Configuration.
            grace_seconds: Not Implemented Currently due to GCP not supporting dynamic
                grace period configuration.
        """
        if grace_seconds != 30:
            self._logger.warning(
                f"Kill grace period of {grace_seconds}s requested, but GCP does not "
                "support dynamic grace period configuration"
            )

        with self._get_client(configuration) as client:
            await run_sync_in_worker_thread(
                self._stop_job,
                client=client,
                project=configuration.project,
                location=configuration.region,
                job_name=infrastructure_pid,
            )

    @staticmethod
    def _stop_job(
        client: Resource,
        project: str,
        location: str,
        job_name: str,
    ):
        """
        Stops/Deletes a Cloud Run job.

        Args:
            client: The googleapiclient.discovery Resource for Cloud Run v2 API.
            project: The GCP Project of the Job.
            location: The GCP Region Location of the Job being stopped.
            job_name: THe Job Name of the Job being stopped.
        """
        try:
            JobV2.delete(
                client=client,
                project=project,
                location=location,
                job_name=job_name,
            )
        except Exception as exc:
            if "does not exist" in str(exc):
                raise InfrastructureNotFound(
                    f"Cannot stop Cloud Run Job; the job name {job_name!r} "
                    "could not be found."
                ) from exc
            raise
