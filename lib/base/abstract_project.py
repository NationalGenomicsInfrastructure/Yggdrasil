import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any

from lib.core_utils.logging_utils import custom_logger
from lib.core_utils.ygg_session import YggSession
from lib.module_utils.hpc_submission_policy import HPCSubmissionPolicy
from lib.module_utils.sjob_manager import SlurmManagerFactory


class AbstractProject(ABC):
    """Abstract base class for realm module projects in the Yggdrasil application.

    The `AbstractProject` serves as a foundational framework for the diverse realms connected by Yggdrasil.
    Each realm (processing module) extends this template, defining its unique processing logic while adhering
    to the core structure laid out by this class.

    This template outlines the common steps and sequences, while allowing flexibility for specific realm implementations.
    It ensures that while each realm follows its own internal rules, they are all integral parts of Yggdrasil's tree
    structure, contributing to the overarching narrative and functionality of the application.

    Attributes:
        sjob_manager (SlurmJobManager): Manages submission and monitoring of Slurm jobs.
        doc (Any): The document representing the project or data to be processed.
        ydm (Any): The database manager (yggdrasil_db_manager) for Yggdrasil-specific database operations.
    """

    def __init__(
        self,
        doc: Any,
        yggdrasil_db_manager: Any,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize the AbstractProject.

        Args:
            doc (Any): The document representing the project (data to be processed).
            yggdrasil_db_manager (Any): The database manager for Yggdrasil-specific database operations.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.sjob_manager: Any = SlurmManagerFactory.get_manager(YggSession.is_dev())
        self.submission_policy: HPCSubmissionPolicy = HPCSubmissionPolicy(
            user_manual_submit=YggSession.is_manual_submit()
        )
        self.doc: Any = doc
        self.ydm: Any = yggdrasil_db_manager
        self.project_id: str = self.doc.get("project_id")
        self.project_name: str = self.doc.get("project_name")
        self.doc_id: str = self.doc.get("_id")
        self.method: str = self.doc.get("details", {}).get(
            "library_construction_method", ""
        )
        self.user_info: dict = {
            "owner": {
                "email": self.doc.get("order_details", {})
                .get("owner", {})
                .get("email", ""),
                "name": self.doc.get("order_details", {})
                .get("owner", {})
                .get("name", ""),
            },
            "bioinformatician": {
                "email": self.doc.get("order_details", {})
                .get("fields", {})
                .get("project_bx_email", ""),
                "name": self.doc.get("order_details", {})
                .get("fields", {})
                .get("project_bx_name", ""),
            },
            "pi": {
                "email": self.doc.get("order_details", {})
                .get("fields", {})
                .get("project_pi_email", ""),
                "name": self.doc.get("order_details", {})
                .get("fields", {})
                .get("project_pi_name", ""),
            },
            "lab": {
                "name": self.doc.get("order_details", {})
                .get("fields", {})
                .get("project_lab_email", ""),
                "email": self.doc.get("order_details", {})
                .get("fields", {})
                .get("project_lab_name", ""),
            },
        }
        sensitive_str = self.doc.get("details", {}).get("sensitive_data", "").lower()
        self.is_sensitive = sensitive_str == "yes"

        self._project_status: str = ""
        # TODO: Eventually replace self.status with self.project_status (in child classes too)
        self.status: str = self.project_status

        self.project_info: dict = {}
        self.samples: list = []
        self.proceed: bool = False  # Default to False; subclasses or checks can set it

    # def setup_project(self):
    #     """Template method defining the steps for project setup."""
    #     self.proceed = self.check_required_fields()
    #     if self.proceed:
    #         self.initialize_project_in_db()
    #         # self._extract_project_specific_info()
    #         # self.extract_samples()
    #     else:
    #         self._logger.error("Cannot proceed due to missing required fields.")

    @property
    def project_status(self):
        return self._project_status

    @project_status.setter
    def project_status(self, new_status):
        self._project_status = new_status
        doc = self.ydm.get_document_by_project_id(self.project_id)
        if not doc:
            self._logger.error(
                f"[{self.project_id}] Cannot set status. Project not found."
            )
            return
        doc.update_project_status(new_status)
        self.ydm.save_document(doc)
        self._logger.info(f"[{self.project_id}] Project status set to '{new_status}'.")

    def initialize_project_in_db(self):
        """
        Initialize the current project in the Yggdrasil database.

        This method checks if the project is already present in the database. If not found,
        a new record is created. If found, the current status is retrieved to determine
        whether further processing should occur. If the project is already completed, the
        method logs a completion message and prevents subsequent actions. When an existing
        project is not completed, it updates or synchronizes relevant metadata and saves
        the changes back to the database.

        Attributes:
            project_id (str): Unique identifier for the project.
            doc_id (str): Identifier for the associated document.
            project_name (str): Name or title of the project.
            method (str): Processing method used for the project.
            user_info (dict): Information about the user initiating or owning the project.
            is_sensitive (bool): Indicates if the project handles sensitive data.
            ydm (YggdrasilDatabaseManager): Database manager instance for performing
                create, update, and read operations.

        Side Effects:
            Logs detailed information about the project status and decisions made during
            initialization. Updates 'proceed' attribute to indicate if further processing
            is allowed.

        Returns:
            None
        """
        existing_document = self.ydm.check_project_exists(self.project_id)
        if not existing_document:
            # Create the project in YggdrasilDB
            self.ydm.create_project(
                self.project_id,
                self.doc_id,
                self.project_name,
                self.method,
                self.user_info,
                self.is_sensitive,
            )
            self._logger.info(f"Project {self.project_id} created in YggdrasilDB.")
            self.proceed = True
        else:
            self._logger.info(
                f"Project {self.project_id} already exists in YggdrasilDB."
            )
            document = self.ydm.get_document_by_project_id(self.project_id)
            if document:
                self.project_status = document.project_status
                if self.project_status == "completed":
                    self._logger.info(
                        f"Project with ID {self.project_id} is already completed. Skipping processing."
                    )
                    self.proceed = False
                else:
                    self._logger.info(
                        f"Project with ID {self.project_id} has status '{self.project_status}' and will be processed."
                    )
                    self.proceed = True
                    # Update/sync the project in YggdrasilDB
                    document.sync_project_metadata(
                        user_info=self.user_info,
                        is_sensitive=self.is_sensitive,
                    )
                    self.ydm.save_document(document)
            else:
                self._logger.error(
                    f"Could not fetch YggdrasilDocument for {self.project_id}."
                )
                self.proceed = False

    def add_samples_to_project_in_db(self):
        """Add samples to the project in the Yggdrasil database."""
        for sample in self.samples:
            self.ydm.add_sample(
                project_id=self.project_id,
                sample_id=sample.id,
                # lib_prep_option=sample.project_info.get("library_prep_option", ""),
                status=sample.status,
            )

    @abstractmethod
    def check_required_fields(self) -> bool:
        """Check if the document contains all required fields.

        Returns:
            bool: True if all required fields are present, False otherwise.
        """
        pass

    @abstractmethod
    async def launch(self) -> None:
        """
        The *old* abstract method. Currently used by concrete classes (TenXProject, SmartSeq3Project, etc.).
        This will eventually be retired / replaced by `launch_template()`.

        ------------------

        Define the main processing logic for the project.

        This method should be implemented by each project class to define how the processing
        of the document unfolds.

        Note:
            Since its course might involve asynchronous operations (e.g., submitting and monitoring jobs),
            this method is defined as asynchronous.
        """
        pass

    @abstractmethod
    def create_slurm_job(self, data: Any) -> str:
        """Create a Slurm job for the given data.

        Args:
            data (Any): The data to create a Slurm job for.

        Returns:
            str: The path to the created Slurm job script.
        """
        pass

    def submit_job(self, script: str) -> Any:
        """Submit a job to the Slurm scheduler using the SlurmJobManager.

        Args:
            script_path (str): The path to the script or command to be submitted as a job.

        Returns:
            Any: The job ID or submission result.
        """
        return self.sjob_manager.submit_job(script)

    def monitor_job(self, job_id: str, sample: Any) -> Any:
        """Monitor the status of a submitted Slurm job using the SlurmJobManager.

        Args:
            job_id (str): The identifier of the submitted job to be monitored.
            sample (Any): The sample object associated with the job.

        Returns:
            Any: The result of the job monitoring.
        """
        return self.sjob_manager.monitor_job(job_id, sample)

    @abstractmethod
    def post_process(self, result: Any) -> None:
        """Handle post-processing specific to the project's outcome.

        Args:
            result (Any): The result from the Slurm job to be post-processed.
        """
        pass

    # ---------------------------------------------------------------
    # New Template Approach
    # ---------------------------------------------------------------

    def fetch_and_merge_sample_info_from_db(self) -> None:
        """
        Re-fetches the project doc from yggdrasilDB, then for each sample in self.samples,
        merges relevant fields like slurm_job_id, status, etc. from the DB record into the
        in-memory sample object.
        """
        self._logger.info(f"[{self.project_id}] Fetching and merging sample info.")
        doc = self.ydm.get_document_by_project_id(self.project_id)
        if not doc:
            self._logger.error(
                f"[{self.project_id}] Cannot fetch project doc. Merge aborted."
            )
            return

        for sample in self.samples:
            sample_doc = doc.get_sample(sample.id)
            if not sample_doc:
                self._logger.warning(
                    f"Sample '{sample.id}' not found in DB doc. Skipping."
                )
                continue

            # For HPC job
            if "slurm_job_id" in sample_doc:
                sample.job_id = sample_doc.get("slurm_job_id")

            # Merging sample's status from DB
            db_status = sample_doc.get("status")
            if db_status and db_status != sample.status:
                # Update local status if different
                self._logger.info(
                    f"Updating sample '{sample.id}' status from '{sample.status}' "
                    f"to '{db_status}' based on DB."
                )
                sample._status = db_status  # or sample.status = db_status

            # Optionally, merge other fields:
            # e.g., "custom_field": sample_doc["custom_field"]

        self._logger.info(f"Fetched HPC info for {len(self.samples)} samples from DB.")

    # async def launch_template(self):
    #     """
    #     The new template method that orchestrates the entire flow.
    #     Eventually, Yggdrasil core can call this instead of the old `launch()`.
    #     For now, it's a separate entry point to avoid breaking existing realms.
    #     """
    #     # 1) Check if we have all required fields
    #     self.proceed = self.check_required_fields()
    #     if not self.proceed:
    #         self._logger.info("Skipping project because required fields are missing.")
    #         return

    #     # 2) Initialize or fetch the project in yggdrasilDB
    #     self.initialize_project_in_db()
    #     if not self.proceed:
    #         self._logger.info("Skipping project after DB initialization check.")
    #         return

    #     # 3) Extract realm-specific samples
    #     # NOTE: Assumes all future realms will be sample-centric
    #     self.samples = self.do_extract_samples()
    #     if not self.samples:
    #         self._logger.info("No samples found to process.")
    #         return

    #     # 4) Add samples to the respective project in yggdrasilDB
    #     self.add_samples_to_project_in_db()

    #     # 5) The main HPC logic or manual submission check
    #     await self.do_run_project()

    #     # 6) Any final steps
    #     self.do_finalize()

    async def launch_template(self):
        # 1) Check required fields
        # self.proceed = self.check_required_fields()
        # if not self.proceed:
        #     self._logger.warning(f"{self.project_id} Missing required fields => skipping.")
        #     return

        # # 2) Initialize or load from DB
        # self.initialize_project_in_db()
        # if not self.proceed:
        #     self._logger.warning(
        #         f"{self.project_id} Skipping after DB initialization check."
        #     )
        #     return

        match self.project_status:
            case "":
                await self._handle_main_flow()
            case "pending":
                await self._handle_main_flow()
            case "partially_completed":
                await self._handle_main_flow()
            case "manually_submitted_samples":
                await self._handle_manually_submitted_flow()
            case "completed":
                self._logger.info(
                    f"Project {self.project_id} is already completed. Nothing to do."
                )
            case _:
                self._logger.warning(
                    f"Project {self.project_id} in unknown status '{self.project_status}'. Skipping."
                )

    # ------------------------------------------------
    # Hooks that Realms override to do actual work
    # ------------------------------------------------

    @abstractmethod
    def do_extract_samples(self) -> list:
        """
        Realm-specific logic for building sample objects from the projectsDB doc.
        Return a list of realm-specific sample objects (e.g. TenXRunSample).
        Store them in self.samples
        """
        pass

    async def do_pre_process_samples(self):
        """
        Optional hook for pre-processing. Realms can override if needed.
        """
        self._logger.info(
            "Default do_pre_process_samples: override in realm if needed."
        )

    async def do_process_samples(self, samples):
        """
        Process samples automatically if realm or sample requires it.
        Realms override if more advanced logic is needed.
        """
        self._logger.info("Default do_process_samples: override in realm.")

    async def do_submit_sample_jobs(self):
        """
        Submit HPC jobs for all samples in the project.
        This method is expected to call sample.submit_job() for each sample.
        Override in realm if needed.
        """
        if not self.samples:
            self._logger.warning("No samples to process.")
            return

        self._logger.info(f"[{self.project_id}] Submitting sample jobs...")
        tasks = [sample.submit_job() for sample in self.samples]
        await asyncio.gather(*tasks)

        # NOTE: Not expected to have any 'failed' samples here, but if we do:
        # TODO: Handle failed samples (e.g. resubmit, notify, etc.)
        failed = [
            sample
            for sample in self.samples
            if sample.status == "job_submission_failed"
        ]
        if failed:
            self._logger.error(
                f"[{self.project_id}] Some samples failed to submit: {[sample.id for sample in failed]}"
            )
        else:
            self._logger.info(f"[{self.project_id}] Sample jobs submitted.")

    async def do_monitor_hpc_jobs(self):
        """
        Concurrently monitor HPC jobs for samples that have been submitted.
        Once done, SlurmJobManager will set each sample's status to 'processed'
        (on success) or 'processing_failed' (on fail).
        This method doesn't call sample.post_process(), that happens later.
        """
        # Gather any samples that are mid-HPC:
        # e.g. auto-submitted, processing, or anything signifying HPC is running
        running_samples = [
            sample
            for sample in self.samples
            if sample.job_id
            and sample.status
            in [
                "auto-submitted",
                "manually_submitted",
                "processing",
            ]  # `processing` is not a status right now
        ]
        if not running_samples:
            self._logger.info(f"No HPC jobs to monitor for project {self.project_id}.")
            return

        tasks = []
        for sample in running_samples:
            job_id = sample.job_id
            self._logger.info(f"Monitoring HPC job {job_id} for sample {sample.id}...")
            # We'll call monitor_job(...) in parallel
            tasks.append(
                asyncio.create_task(self.sjob_manager.monitor_job(job_id, sample))
            )

        self._logger.info(f"Created {len(tasks)} monitoring tasks for HPC jobs.")
        await asyncio.gather(*tasks)
        self._logger.info("All HPC monitoring tasks completed.")

        # Now each sample is either 'processed' or 'processing_failed'
        # HPC is done for those that were monitored.

    async def do_post_process_samples(self):
        """
        A unified, generic post-processing flow:
        1) Skip samples already 'completed' or in a fail state.
        2) Post-process those in 'processed'.
        3) Report any samples that remain in other statuses (for debugging).
        """

        # Collect processed samples for post-processing
        processed_samples = [
            sample for sample in self.samples if sample.status == "processed"
        ]

        if processed_samples:
            self._logger.info(f"Post-processing {len(processed_samples)} samples.")
            for sample in processed_samples:
                old_status = sample.status
                sample.post_process()  # Sample status will become either 'completed' or 'post_processing_failed'
                self._logger.info(
                    f"Sample {sample.id} status went from '{old_status}' to '{sample.status}'."
                )
        else:
            self._logger.info("No samples in 'processed' => skipping post-process.")

        # List any samples that failed post-processing
        failed_post_processing = [
            sample
            for sample in self.samples
            if sample.status == "post_processing_failed"
        ]
        if failed_post_processing:
            self._logger.warning(
                f"Post-processing failed for {len(failed_post_processing)} samples:"
            )
            for sample in failed_post_processing:
                self._logger.warning(f" - Sample {sample.id} failed post-processing.")

        # List any samples that DO NOT have status 'completed' or 'post_processing_failed'
        leftover = []
        for sample in self.samples:
            if sample.status not in [
                "completed",
                "post_processing_failed",
            ]:
                leftover.append(sample)

        if leftover:
            self._logger.info(
                "Some samples were not post-processed due to their status:"
            )
            for sample in leftover:
                self._logger.info(f" - Sample {sample.id} is '{sample.status}'")

        self._logger.info("Post-process step complete.")

    async def do_finalize_project(self):
        """
        Some final steps to wrap up a project and set project status.
        Realms or base can override/extend.
        """
        self._logger.info(
            "Default do_finalize_project: override or extend in realm if needed."
        )
        self.project_status = "completed"

    # ------------------------------------------------
    # State handlers: break down further if needed
    # ------------------------------------------------

    async def _handle_main_flow(self):
        """
        Handles a brand-new or not-yet-submitted project.
        We'll extract samples, pre-process them,
        then either auto-submit HPC or mark them for manual submission.
        Finally, we decide whether we do HPC monitoring or update the project status
        to 'manually_submitted_samples'.
        """
        self._logger.info(f"[{self.project_id}] Handling main flow for project.")

        # 1) Extract & register samples
        self.do_extract_samples()
        if not self.samples:
            self._logger.warning("No samples found => nothing to do.")
            return

        self.fetch_and_merge_sample_info_from_db()
        self.add_samples_to_project_in_db()

        # 2) Pre-process
        await self.do_pre_process_samples()
        if not self.samples:
            self._logger.warning(
                "No samples left after pre-processing => nothing to do."
            )
            return

        # 3) Decide auto vs. manual
        #    If auto-submission == True, we call do_submit_sample_jobs()
        #    If not, we set project => "manually_submitted_samples" so user can do HPC externally
        # auto_submit = self.doc.get("pipeline_info", {}).get("submit", True)
        # if auto_submit:
        if self.submission_policy.should_auto_submit():
            self._logger.info("Auto-submitting HPC jobs for all samples.")
            await self.do_submit_sample_jobs()

            # Optionally immediately monitor them here or set a new status
            # that triggers HPC monitoring in a separate pass
            await self.do_monitor_hpc_jobs()  # if you want to do it now

            processed_samples = [
                sample for sample in self.samples if sample.status == "processed"
            ]
            self._logger.info(
                f"Samples that finished successfully: "
                f"{[sample.id for sample in processed_samples]}\n"
            )

            # Then proceed to post-process
            await self.do_post_process_samples()
            await self.do_finalize_project()
        else:
            self._logger.info("Manual submission required => no HPC submission now.")
            self.project_status = "manually_submitted_samples"

    async def _handle_manually_submitted_flow(self):
        """
        The user has manually submitted HPC jobs for these samples.
        We'll re-extract, load job IDs, monitor, then post-process once done.
        """
        self._logger.info(
            f"Handling 'manually_submitted_samples' flow for project {self.project_id}."
        )

        # 1) Extract or re-extract samples
        self.do_extract_samples()
        if not self.samples:
            self._logger.warning("No samples found => nothing to do.")
            return

        # 2) Merge any HPC job IDs or updated statuses from DB
        #    so each sample knows job_id, partial states, etc.
        self.fetch_and_merge_sample_info_from_db()

        # 3) Monitor HPC jobs
        await self.do_monitor_hpc_jobs()

        processed_samples = [
            sample for sample in self.samples if sample.status == "processed"
        ]
        self._logger.info(
            f"Samples that finished successfully: "
            f"{[sample.id for sample in processed_samples]}\n"
        )

        # If HPC is done for all, post-process and finalize
        await self.do_post_process_samples()
        await self.do_finalize_project()
        await self.do_finalize_project()
