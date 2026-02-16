import datetime
from typing import Any

from lib.core_utils.logging_utils import custom_logger

logging = custom_logger(__name__.split(".")[-1])


class YggdrasilDocument:
    """Represents a Yggdrasil project document.

    Attributes:
        _id (str): The unique identifier of the project (same as project_id).
        projects_reference (str): Reference to the original project document.
        method (str): The library construction method.
        project_id (str): The project ID.
        project_name (str): The name of the project.
        status (str): The current status of the project.
        start_date (str): ISO formatted start date and time.
        end_date (str): ISO formatted end date and time.
        samples (List[Dict[str, Any]]): List of samples associated with the project.
    """

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "YggdrasilDocument":
        """Creates a YggdrasilDocument instance from a dictionary.

        Args:
            data (Dict[str, Any]): The dictionary containing document data.

        Returns:
            YggdrasilDocument: The created instance.
        """
        instance = cls(
            project_id=data.get("project_id", ""),
            projects_reference=data.get("projects_reference", ""),
            project_name=data.get("project_name", ""),
            method=data.get("method", ""),
        )
        # Project-level fields
        instance.project_status = data.get("project_status", "ongoing")
        instance.start_date = data.get(
            "start_date", datetime.datetime.now().isoformat()
        )
        instance.end_date = data.get("end_date", "")

        # Samples
        instance.samples = data.get("samples", [])

        # User info
        instance.user_info = data.get("user_info", {})

        # Delivery info
        instance.delivery_info = data.get("delivery_info", {})
        if "delivery_results" not in instance.delivery_info:
            # Always ensure we have a list for delivery_results
            instance.delivery_info["delivery_results"] = []

        # NGI report array
        instance.ngi_report = data.get("ngi_report", [])

        return instance

    def __init__(
        self, project_id: str, projects_reference: str, project_name: str, method: str
    ) -> None:
        """Initializes a new YggdrasilDocument instance.

        Args:
            project_id (str): The project ID.
            projects_reference (str): Reference to the original project document.
            project_name (str): The project name.
            method (str): The library construction method.
        """
        self._id: str = project_id
        self.projects_reference: str = projects_reference
        self.method: str = method
        self.project_id: str = project_id
        self.project_name: str = project_name

        # Project lifecycle
        self.project_status: str = "ongoing"
        self.start_date: str = datetime.datetime.now().isoformat()
        self.end_date: str = ""

        # Samples & Delivery
        self.samples: list[dict[str, Any]] = []
        self.delivery_info: dict[str, Any] = {"delivery_results": []}
        self.ngi_report: list[dict[str, Any]] = []
        self.user_info: dict[str, dict[str, str | None]] = {}

    def to_dict(self) -> dict[str, Any]:
        """Converts the YggdrasilDocument to a dictionary.

        Returns:
            Dict[str, Any]: A dictionary representation of the document.
        """
        return {
            "_id": self._id,
            "projects_reference": self.projects_reference,
            "method": self.method,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "project_status": self.project_status,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "samples": self.samples,
            "delivery_info": self.delivery_info,
            "ngi_report": self.ngi_report,
            "user_info": self.user_info,
        }

    # ---------------------------
    # USER INFO
    # ---------------------------

    def set_user_info(self, updated_info: dict[str, dict[str, str | None]]) -> None:
        """
        Updates self.user_info with the nested dictionary provided.

        Example updated_info:
        {
            "owner": {
            "email": "owner@host.org",
            "name": "Owner Name"
            },
            "pi": {
            "email": "pi@host.org",
            "name": "PI Name"
            }
        }
        """
        for role, sub_dict in updated_info.items():
            if role not in self.user_info:
                # If the doc didn't have that role yet, create a blank dict
                self.user_info[role] = {}
            # Copy keys like "email", "name"
            for key, val in sub_dict.items():
                self.user_info[role][key] = val or ""

    # ------------------------------------------------------------------------
    # SAMPLES
    # ------------------------------------------------------------------------

    def add_sample(
        self,
        sample_id: str,
        slurm_job_id: str | None = None,
        # lib_prep_option: str,
        status: str = "pending",
        flowcell_ids_processed_for: list[str] | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> None:
        """Adds or updates a sample to the document.

        Args:
            sample_id (str): The sample ID.
            slurm_job_id (str, optional): The SLURM job ID, if any.
            lib_prep_option (str): The library preparation option.
            status (str, optional): The status of the sample. Defaults to "pending".
            flowcell_ids_processed_for (List[str], optional): Flowcell IDs the sample has been processed for.
            start_time (str, optional): Start time of the sample processing.
            end_time (str, optional): End time of the sample processing.
        """
        existing_sample = self.get_sample(sample_id)
        if existing_sample:
            # Update existing sample
            existing_sample["status"] = status
            if flowcell_ids_processed_for:
                existing_sample.setdefault("flowcell_ids_processed_for", [])
                existing_sample["flowcell_ids_processed_for"].extend(
                    flowcell_ids_processed_for
                )
                # Remove duplicates
                existing_sample["flowcell_ids_processed_for"] = list(
                    set(existing_sample["flowcell_ids_processed_for"])
                )
            if start_time:
                existing_sample["start_time"] = start_time
            if end_time:
                existing_sample["end_time"] = end_time
            # logging.debug(f"Updated sample: {existing_sample}")
        else:
            # Add new sample
            sample = {
                "sample_id": sample_id,
                "status": status,
                "slurm_job_id": slurm_job_id or "",
                # "lib_prep_option": lib_prep_option,
                "start_time": start_time or datetime.datetime.now().isoformat(),
                "end_time": end_time or "",
                "flowcell_ids_processed_for": flowcell_ids_processed_for or [],
                "QC": "",  # Appropriate statuses "Pending"/"Passed"/"Failed"; should be updated later
                "delivered": False,
            }
            self.samples.append(sample)
            # logging.debug(f"Added sample: {sample}")

        # self.check_project_completion() # NOTE: Should this be here?

    def get_sample(self, sample_id: str) -> dict[str, Any] | None:
        """Retrieves a specific sample from the samples list by its ID.

        Args:
            sample_id (str): The sample ID to retrieve.

        Returns:
            Optional[Dict[str, Any]]: The sample dictionary if found, else None.
        """
        for sample in self.samples:
            if sample["sample_id"] == sample_id:
                return sample
        return None

    def update_sample_status(self, sample_id: str, status: str) -> bool:
        """Updates the status of a specific sample.

        Args:
            sample_id (str): The sample ID to update.
            status (str): The new status of the sample.
        """
        sample = self.get_sample(sample_id)
        if not sample:
            logging.error(
                f"Sample '{sample_id}' not found in project '{self.project_id}'."
            )
            return False

        sample["status"] = status
        current_time = datetime.datetime.now().isoformat()

        # TODO: This is not correct. The start time should be set when the sample is actually started.
        if status in ["processing", "running", "pre_processing", "post_processing"]:
            sample["start_time"] = current_time
        elif status in [
            "completed",
            "pre_processing_failed",
            "processing_failed",
            "post_processing_failed",
            "aborted",
        ]:
            sample["end_time"] = current_time

        # Check if the project status needs to be updated
        self.check_project_completion()

        return True

    # NOTE: This is not supposed to be used by Yggdrasil, but by the user interface
    # NOTE: When a responsible reviews the sample, Genstat will update the QC status
    def set_sample_qc_status(self, sample_id: str, qc_value: str) -> None:
        """
        Sets the QC status (Passed/Failed/Pending) for a sample.
        """
        sample = self.get_sample(sample_id)
        if not sample:
            logging.error(f"Cannot set QC: sample '{sample_id}' not found.")
            return
        sample["QC"] = qc_value

    def mark_sample_as_delivered(self, sample_id: str) -> None:
        """
        Mark a sample as delivered (set delivered=True).
        """
        sample = self.get_sample(sample_id)
        if not sample:
            logging.error(f"Cannot mark delivered: sample '{sample_id}' not found.")
            return
        sample["delivered"] = True

    def get_sample_status(self, sample_id: str) -> str | None:
        sample = self.get_sample(sample_id)
        if sample:
            return sample.get("status")
        return None

    # TODO: `set_sample_qc_status` and `mark_sample_as_delivered` should become
    #       convenience methods in YggdrasilDBManager
    def update_sample_field(self, sample_id: str, field_name: str, value: Any) -> bool:
        """
        Updates or sets the given field in a sample's dictionary.

        Args:
            sample_id (str): The sample ID to update.
            field_name (str): The name of the field to set (e.g. 'slurm_job_id').
            value (Any): The new value for that field (e.g. a string job ID).

        Returns:
            bool: True if the sample was found and updated, False otherwise.
        """
        if field_name == "status":
            logging.warning(
                "Attempted to update sample status via 'update_sample_field';"
            )
            if value:
                logging.info("Attempting to use 'update_sample_status'.")
                return self.update_sample_status(sample_id, value)
            return False

        sample = self.get_sample(sample_id)
        if not sample:
            logging.error(
                f"Cannot update field '{field_name}' for sample '{sample_id}' "
                f"in project '{self.project_id}': sample not found."
            )
            return False

        sample[field_name] = value
        logging.debug(
            f"Updated sample '{sample_id}' in project '{self.project_id}' with "
            f"'{field_name}': {value}"
        )

        # Check if the project status needs to be updated
        self.check_project_completion()

        return True

    # ------------------------------------------------------------------------
    # PROJECT
    # ------------------------------------------------------------------------

    def sync_project_metadata(
        self,
        user_info: dict[str, dict[str, str | None]],
        is_sensitive: bool,
    ) -> None:
        """
        Updates user_info & sensitive fields in the document.
        """
        # Update user_info
        self.set_user_info(user_info)

        # Update sensitive
        self.delivery_info["sensitive"] = is_sensitive

        # NOTE: Hardcoded for now, may be configurable in the future
        # self.delivery_info["partial_delivery_allowed"] = False

    def update_project_status(self, new_status: str) -> None:
        """Updates the overall project status. If 'completed', set end_date."""
        self.project_status = new_status
        if new_status in ["completed", "partially_completed"] and not self.end_date:
            self.end_date = datetime.datetime.now().isoformat()
        elif new_status in ["processing", "failed", "ongoing"]:
            self.end_date = ""  # we clear end_date if not fully done

    def check_project_completion(self) -> None:
        """
        Determines the project status based on its samples.

        Logic:
            1) If any sample is active (e.g. 'processing', 'running', etc.),
            project -> 'processing'
            2) Else if every sample is finished ('completed' or 'aborted'),
            project -> 'completed'
            3) Else if every sample is not_yet_started ('pending', 'unsequenced', etc.),
            project -> 'pending'
            4) Otherwise, project -> 'partially_completed'
        """
        # You may adjust these sets to match your real usage
        active_sample_statuses = {
            "initialized",
            "processing",
            "pre_processing",
            "post_processing",
            "requires_manual_submission",
        }
        finished_sample_statuses = {"completed", "aborted"}
        not_yet_started_statuses = {"pending", "unsequenced"}

        # Collect all sample statuses into a set for quick membership checks
        sample_statuses = [sample["status"] for sample in self.samples]
        unique_sample_statuses = set(sample_statuses)

        # 1) If any sample is "active" => project is 'processing'
        if any(
            sample_status in active_sample_statuses
            for sample_status in unique_sample_statuses
        ):
            # self.project_status = "processing"
            # self.end_date = ""  # not fully completed
            self.update_project_status("processing")
            return

        # 2) If ALL samples are "finished" => 'completed'
        if all(
            sample_status in finished_sample_statuses
            for sample_status in unique_sample_statuses
        ):
            # self.project_status = "completed"
            # if not self.end_date:
            #     self.end_date = datetime.datetime.now().isoformat()
            self.update_project_status("processing")
            return

        # 3) If ALL samples are "not_yet_started" => 'pending'
        if all(
            sample_status in not_yet_started_statuses
            for sample_status in unique_sample_statuses
        ):
            # self.project_status = "pending"
            # self.end_date = ""
            self.update_project_status("pending")
            return

        # 4) Otherwise => 'partially_completed'
        # means no sample is actively running, but at least one is neither finished nor not_yet_started
        # self.project_status = "partially_completed"
        # self.end_date = ""
        self.update_project_status("partially_completed")

    def get_project_status(self) -> str | None:
        """Retrieves the status of a project.

        Returns:
            Optional[str]: The status of the project.
        """
        return self.project_status

    # ------------------------------------------------------------------------
    # NGI REPORT MANAGEMENT
    # ------------------------------------------------------------------------

    def add_ngi_report_entry(self, report_data: dict[str, Any]) -> bool:
        """
        Append a new record to `ngi_report`.
        Example `report_data`:
        {
          "file_name": "P12345_ngi_report.html",
          "date_created": "2025-02-02_10:20:30",
          "signee": "",
          "date_signed": "",
          "rejected": False,
          "samples_included": [...]
        }
        """
        required_keys = {
            "file_name",
            "date_created",
            "signee",
            "date_signed",
            "rejected",
            "samples_included",
        }
        if isinstance(report_data, dict) and required_keys.issubset(report_data.keys()):
            self.ngi_report.append(report_data)
            return True
        else:
            logging.error("Invalid report_data format or missing required keys.")
            return False

    # ------------------------------------------------------------------------
    # DELIVERY INFO / DELIVERY EVENTS
    # ------------------------------------------------------------------------

    def add_delivery_entry(self, delivery_data: dict[str, Any]) -> None:
        """
        Add a new entry to `delivery_info.delivery_results`.
        Example `delivery_data`:
        {
          "dds_project_id": "DDS123",
          "date_uploaded": "2025-02-10_14:01:00",
          "date_released": "2025-02-10_17:25:00",
          "samples_included": ["P12345_101", "P12345_102"],
          "total_volume": "100GB"
        }
        """
        if "delivery_results" not in self.delivery_info:
            self.delivery_info["delivery_results"] = []
        self.delivery_info["delivery_results"].append(delivery_data)

    # NOTE: Not sure we need it and if [-1] is consistent to getting the last entry
    # def get_last_delivery_entry(self) -> Optional[Dict[str, Any]]:
    #     """Return the last delivery entry from delivery_info."""
    #     if "delivery_results" in self.delivery_info:
    #         return self.delivery_info["delivery_results"][-1]
    #     return None

    def get_delivery_status(self) -> str:
        """Return the current delivery phase/status from delivery_info."""
        return self.delivery_info.get("status", "")

    def set_delivery_status(self, new_status: str) -> None:
        """Update the delivery status in delivery_info."""
        self.delivery_info["status"] = new_status
