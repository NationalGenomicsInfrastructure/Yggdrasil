import asyncio
import datetime
import logging
import subprocess
from pathlib import Path

from json_logic import jsonLogic

from lib.core_utils.config_loader import ConfigLoader
from lib.core_utils.logging_utils import custom_logger
from lib.couchdb.yggdrasil_document import YggdrasilDocument
from lib.module_utils.report_transfer import transfer_report


# TODO: Potentially inherit from the AbstractProject class if it makes sense, and rename the base class to not be bound to projects.
class DeliveryManager:
    def __init__(
        self,
        doc: YggdrasilDocument,
        ydm,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the DeliveryManager with the given project document and YggdrasilDB manager.

        Args:
            doc [YggdrasilDocument]: The Yggdrasil project document.
            ydm: The YggdrasilDBManager instance.

        The `doc` is expected to contain at least:
        - "project_id"
        - "delivery_info": a dictionary with keys like:
          * "ngi_report_signee": str or ""
          * "taca_config": str
          * "sensitive": bool
          * "status": str (e.g. "ready-for-delivery", "ngi_report_uploaded_for_signing", "delivered", etc.)
          * "deliver_flowcell": bool
        - "samples": a list of samples each with at least "sample_id" and "QC" (e.g. "OK", "re-prep", "re-sequence")

        The `ydm` manager is used to update the doc as needed.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.doc = doc
        self.ydm = ydm
        self.proceed = True
        self.project_id = self.doc.project_id

        # If doc doesn't have delivery_info or project_id, no reason to proceed
        # TODO: Add more checks as needed - like if samples are missing, etc.
        if not self.project_id or not self.doc.delivery_info:
            self.proceed = False

    async def launch(self):
        """
        Launch the delivery process. This method:
        - Loads jsonLogic rules for decision making.
        - Applies the rules to decide which actions to take.
        - Executes those actions (e.g. generate NGI report, wait for signing, stage with TACA, deliver).
        """
        if not self.proceed:
            self._logger.info(
                f"[{self.project_id}] DeliveryManager: Document not ready or missing fields. Aborting."
            )
            return

        if not self.doc.delivery_info:
            self._logger.info(
                f"[{self.project_id}] DeliveryManager: No delivery_info found. Aborting."
            )
            return

        # 1) Load and apply jsonLogic rules
        rules = self.load_rules()
        data_for_rules = {
            "delivery_info": self.doc.delivery_info,
            "samples": self.doc.samples,
            "method": self.doc.method,
            "project_id": self.doc.project_id,
            "has_ngi_report": bool(self.doc.ngi_report),
            "ngi_report_latest": self.doc.ngi_report[-1] if self.doc.ngi_report else {},
        }

        decision = jsonLogic(rules, data_for_rules)  # type: ignore
        # `decision` should now contain something like:
        # {
        #   "actions": ["generate_report", "upload_report"], or
        #   "actions": ["proceed_taca"], etc.
        #   "parameters": {... optional ...}
        # }
        # NOTE: Design the rules to return a structure indicating what to do next.

        if not decision:  # or "actions" not in decision:
            self._logger.warning(
                f"[{self.project_id}] No actions returned from decision rules. Nothing to do."
            )
            return

        if not isinstance(decision, list):
            self._logger.warning(
                f"[{self.project_id}] Decision is not a list of actions. Received: {decision}"
            )
            return

        # 2) Execute the actions returned by the jsonLogic decision
        print(decision)
        actions = decision  # ["actions"]
        # Possible actions we might have:
        # - "generate_ngi_report": generate NGI report and transfer it for signing
        # - "wait_for_signing": means we generated report, no signee yet
        # - "proceed_delivery": means NGI report signed, can run TACA staging/delivery
        # - "finish": means everything delivered
        # etc.

        # TODO: If complexity grows, map actions to methods.
        for action in actions:
            if action == "generate_ngi_report":
                await self.perform_ngi_report_generation_and_upload()
            elif action == "wait_for_signing":
                self._logger.info(
                    f"[{self.project_id}] DeliveryManager: Waiting for NGI report signing."
                )
                # TODO: Could check if it's been X days since report generation and remind the responsible person
            elif action == "proceed_delivery":
                # This means NGI report signed and we can do TACA steps
                await self.perform_taca_delivery_steps()

            # elif action == "update_status":
            #     # Possibly generic action if rules say so
            #     new_status = decision.get("new_status", None)
            #     if new_status:
            #         self.update_delivery_info(
            #             self.doc["project_id"], {"status": new_status}
            #         )

            elif action == "finish":
                self._logger.info(
                    f"[{self.project_id}] DeliveryRealm: Execution Completed."
                )
            else:
                self._logger.info(
                    f"[{self.project_id}] Unrecognized action '{action}', skipping."
                )

    def load_rules(self):
        """
        Rules Structure:
        - If any sample has QC = "Pending", do nothing.
        - Else:
            - If NGI report is not signed:
                - If status is "ready-for-delivery", generate NGI report.
                - Else, wait for signing.
            - Else:
                - If status is "ready-for-delivery", proceed with delivery.
                - Else, finish the process.
        """
        rules = {
            "if": [
                {"some": [{"var": "samples"}, {"==": [{"var": "QC"}, "Pending"]}]},
                [],
                {
                    "if": [
                        {"var": "has_ngi_report"},
                        {
                            "if": [
                                {
                                    "and": [
                                        {
                                            "!=": [
                                                {"var": "ngi_report_latest.signee"},
                                                "",
                                            ]
                                        },
                                        {
                                            "==": [
                                                {"var": "ngi_report_latest.rejected"},
                                                False,
                                            ]
                                        },
                                    ]
                                },
                                ["proceed_delivery"],
                                ["wait_for_signing"],
                            ]
                        },
                        {
                            "if": [
                                {"var": "delivery_info.partial_delivery_allowed"},
                                {
                                    "if": [
                                        {
                                            "some": [
                                                {"var": "samples"},
                                                {"==": [{"var": "QC"}, "Passed"]},
                                            ]
                                        },
                                        ["generate_ngi_report"],
                                        ["finish_no_passed_samples"],
                                    ]
                                },
                                {
                                    "if": [
                                        {
                                            "and": [
                                                {
                                                    "!": {
                                                        "some": [
                                                            {"var": "samples"},
                                                            {
                                                                "!": {
                                                                    "in": [
                                                                        {"var": "QC"},
                                                                        [
                                                                            "Passed",
                                                                            "Aborted",
                                                                        ],
                                                                    ]
                                                                }
                                                            },
                                                        ]
                                                    }
                                                },
                                                {
                                                    "some": [
                                                        {"var": "samples"},
                                                        {
                                                            "==": [
                                                                {"var": "QC"},
                                                                "Passed",
                                                            ]
                                                        },
                                                    ]
                                                },
                                            ]
                                        },
                                        ["generate_ngi_report"],
                                        {
                                            "if": [
                                                {
                                                    "!": {
                                                        "some": [
                                                            {"var": "samples"},
                                                            {
                                                                "!": {
                                                                    "==": [
                                                                        {"var": "QC"},
                                                                        "Aborted",
                                                                    ]
                                                                }
                                                            },
                                                        ]
                                                    }
                                                },
                                                ["finish_all_samples_aborted"],
                                                ["finish_some_samples_failed"],
                                            ]
                                        },
                                    ]
                                },
                            ]
                        },
                    ]
                },
            ]
        }

        #     "if": [
        #         {"some": [{"var": "samples"}, {"==": [{"var": "QC"}, "Pending"]}]},
        #         [],
        #         {
        #             "if": [
        #                 {"var": "delivery_info.partial_delivery_allowed"},
        #                 {
        #                     "if": [
        #                         {
        #                             "some": [
        #                                 {"var": "samples"},
        #                                 {"==": [{"var": "QC"}, "Passed"]},
        #                             ]
        #                         },
        #                         ["generate_ngi_report"],
        #                         ["finish_no_passed_samples"],
        #                     ]
        #                 },
        #                 {
        #                     "if": [
        #                         {
        #                             "and": [
        #                                 {
        #                                     "!": {
        #                                         "some": [
        #                                             {"var": "samples"},
        #                                             {
        #                                                 "!": {
        #                                                     "in": [
        #                                                         {"var": "QC"},
        #                                                         ["Passed", "Aborted"],
        #                                                     ]
        #                                                 }
        #                                             },
        #                                         ]
        #                                     }
        #                                 },
        #                                 {
        #                                     "some": [
        #                                         {"var": "samples"},
        #                                         {"==": [{"var": "QC"}, "Passed"]},
        #                                     ]
        #                                 },
        #                             ]
        #                         },
        #                         ["generate_ngi_report"],
        #                         {
        #                             "if": [
        #                                 {
        #                                     "!": {
        #                                         "some": [
        #                                             {"var": "samples"},
        #                                             {
        #                                                 "!": {
        #                                                     "==": [
        #                                                         {"var": "QC"},
        #                                                         "Aborted",
        #                                                     ]
        #                                                 }
        #                                             },
        #                                         ]
        #                                     }
        #                                 },
        #                                 ["finish_all_samples_aborted"],
        #                                 ["finish_some_samples_failed"],
        #                             ]
        #                         },
        #                     ]
        #                 },
        #             ]
        #         },
        #     ]
        # }
        return rules

    async def perform_ngi_report_generation_and_upload(self):
        """
        Generate the NGI report and transfer it to ngi-internal for signing, then update doc status.
        """
        # 1) Determine which samples have Passed QC
        included_samples = [
            s["sample_id"] for s in self.doc.samples if s.get("QC") == "Passed"
        ]

        if not included_samples:
            self._logger.warning(
                f"[{self.project_id}] No 'QC=Passed' samples; skipping NGI report generation."
            )
            return

        # 2) Load base analysis directory from config
        config = ConfigLoader().load_config("delivery_config.json")
        base_analysis_dir = config.get("NGI_ANALYSIS_DIR")
        if not base_analysis_dir:
            self._logger.error(
                f"[{self.project_id}] 'NGI_ANALYSIS_DIR' not found in delivery_config.json."
            )
            return

        # 3) Construct the project directory path
        project_analysis_dir = Path(base_analysis_dir) / self.project_id
        if not project_analysis_dir.exists():
            self._logger.error(
                f"[{self.project_id}] Project directory '{project_analysis_dir}' not found; cannot generate NGI report."
            )
            return

        # 4) Generate the NGI report
        # user_name = "Firstname Lastname"  # or a placeholder / real user if available
        # report_success = generate_ngi_report(
        #     project_path=str(project_analysis_dir),
        #     project_id=self.project_id,
        #     user_name=user_name,
        #     sample_list=included_samples,
        # )
        report_success = True  # For testing
        if not report_success:
            self._logger.error(f"[{self.project_id}] NGI report generation failed.")
            return

        self._logger.info(
            f"[{self.project_id}] NGI report generated in '{project_analysis_dir}'"
        )

        # 5) Check the final report path
        #    Use project_name if you have one; fallback to project_id if needed
        # TODO: Include the project name in the doc
        # project_label = self.doc.project_name or self.project_id
        file_name = f"{self.doc.project_name}_project_summary.html"
        final_report_path = project_analysis_dir / "reports" / file_name

        if not final_report_path.exists():
            self._logger.error(
                f"[{self.project_id}] NGI report '{final_report_path}' not found; generation step failed unexpectedly."
            )
            return

        # 6) Transfer the report to ngi-internal for signing
        transfer_ok = transfer_report(
            report_path=final_report_path, project_id=self.project_id, sample_id=None
        )
        if not transfer_ok:
            self._logger.error(
                f"[{self.project_id}] Failed to transfer NGI report for signing."
            )
            return

        self._logger.info(f"[{self.project_id}] NGI report transferred for signing.")

        # 7) Fetch doc again, add NGI report entry, update status, and save

        # Create an NGI report record to track it in doc.ngi_report
        new_report_data = {
            "file_name": file_name,
            "date_created": datetime.datetime.now().isoformat(),
            "signee": "",
            "date_signed": "",
            "rejected": False,
            "samples_included": included_samples,
        }

        self.ydm.add_ngi_report_entry(self.project_id, new_report_data)

    async def perform_taca_delivery_steps(self):
        """
        Perform TACA staging and DDS delivery steps after NGI report is signed.
        """
        delivery_config = ConfigLoader().load_config("delivery_config.json")
        method = self.doc.method  # e.g. "SmartSeq 3" or "10X: 3GEX (GEM-X)"
        taca_config_per_method = delivery_config.get("taca_config_per_method", {})

        # 1) Attempt to find a TACA config for the doc.method (prefix or exact match)
        taca_config = None
        if method in taca_config_per_method:
            # direct exact match
            taca_config = taca_config_per_method[method]
        else:
            # prefix match
            # NOTE: This might or might not work as expected
            # e.g. 10X ATAC will match the TACA config, but does it have the same delivery conditions with other 10X methods?
            for known_method, path in taca_config_per_method.items():
                if method.startswith(known_method):
                    taca_config = path
                    break

        if not taca_config:
            self._logger.warning(
                f"[{self.project_id}] No TACA config found for method '{method}'. Cannot proceed."
            )
            # TODO: Notify someone about this issue (Slack?)
            return

        self._logger.info(
            f"[{self.project_id}] Using TACA config '{taca_config}' for method '{method}'."
        )

        # 2) Check 'sensitive' flag
        sensitive = self.doc.delivery_info.get("sensitive", True)
        sensitivity_flag = "--no-sensitive" if not sensitive else "--sensitive"

        # 3) Possibly gather flowcells if deliver_flowcell is True
        deliver_flowcell = self.doc.delivery_info.get("deliver_flowcell", False)
        fc_delivery_flag = ""
        if deliver_flowcell:
            fc_ids = list(
                {
                    fc_id
                    for sample in self.doc.samples
                    for fc_id in sample.get("flowcell_ids_processed_for", [])
                }
            )
            if fc_ids:
                # TODO:
                # TODO: Make sure TACA accepts multiple flowcell IDs!!!!
                # TODO:
                fc_delivery_flag = f"--fc-delivery {' '.join(fc_ids)}"

        # 4) Stage data with TACA
        stage_cmd = (
            f"source /path/to/latest/conf/sourceme_sthlm.sh && "
            f"source activate NGI && "
            f"taca -c {taca_config} deliver --ignore-analysis-status "
            f"--stage_only {fc_delivery_flag} project {self.project_id}"
        )
        print(stage_cmd)

        # if await self.run_cmd(["bash", "-c", stage_cmd]) != 0:
        #     self._logger.error(f"[{self.project_id}] Staging failed.")
        #     return

        # 5) Emails from user_info
        # TODO: Figure out how to insert these emails in the TACA command
        pi_email, contact_email, bioinfo_email = self.fetch_project_contact_emails()
        if not pi_email:
            self._logger.error(
                f"[{self.project_id}] No PI email found, cannot proceed with DDS steps."
            )
            return

        # 6) DDS upload step
        # TODO: How do we add the other emails in TACA? Check the command syntax and ask the production team.
        # TODO: May need to upload specific samples if not all are to be delivered or some have been delivered.
        upload_cmd = (
            f"source /path/to/latest/conf/sourceme_sthlm.sh && "
            f"source activate NGI && "
            f"taca -c {taca_config} deliver --cluster dds project "
            f"{sensitivity_flag} {self.project_id}"
        )
        print(upload_cmd)

        # if await self.run_cmd(["bash", "-c", upload_cmd]) != 0:
        #     self._logger.error(f"[{self.project_id}] DDS upload failed.")
        #     return

        # 7) Release to user
        # TODO: Suppose we got dds_project_id stored in doc now. If not, add a method to fetch or store it.
        dds_project_id = self.doc.delivery_info.get("dds_project_id", "DDS123")
        release_cmd = (
            f"source /path/to/latest/conf/sourceme_sthlm.sh && "
            f"source activate NGI && "
            f"taca -c {taca_config} deliver --cluster dds release-dds-project "
            f"{sensitivity_flag} --dds_project {dds_project_id} --no-dds-mail {self.project_id}"
        )
        print(release_cmd)

        # if await self.run_cmd(["bash", "-c", release_cmd]) != 0:
        #     self._logger.error(f"[{self.project_id}] DDS release failed.")
        #     return

        # 8) Mark final status and insert a 'delivery_results' entry
        self.log_and_store_delivery_result()

        self._logger.info(
            f"[{self.project_id}] TACA staging/upload steps completed successfully."
        )

        # self.update_delivery_info(self.project_id, {"status": "delivered"})

    def log_and_store_delivery_result(self):
        """
        Creates/updates an entry in the doc.delivery_info['delivery_results'] array
        with basic info about this delivery event.
        """
        # Determine which samples were included in this delivery
        # NOTE: Might need to do this earlier if we need to specify samples in the TACA command
        samples_delivered = [
            sample["sample_id"]
            for sample in self.doc.samples
            if sample.get("QC") == "Passed" and not sample.get("delivered")
        ]
        if not samples_delivered:
            self._logger.warning(
                f"[{self.project_id}] No new samples to deliver?! Possibly already delivered."
            )
        else:
            # Mark them as delivered
            for sample in self.doc.samples:
                if sample["sample_id"] in samples_delivered:
                    sample["delivered"] = True

        new_delivery_data = {
            "dds_project_id": self.doc.delivery_info.get("dds_project_id", "DDS123"),
            "date_uploaded": datetime.datetime.now().isoformat(timespec="seconds"),
            "date_released": datetime.datetime.now().isoformat(timespec="seconds"),
            "samples_included": samples_delivered,
            "total_volume": "unknown",  # or calculate if you want
        }

        # Ensure we have a list
        if "delivery_results" not in self.doc.delivery_info:
            self.doc.delivery_info["delivery_results"] = []

        self.doc.delivery_info["delivery_results"].append(new_delivery_data)

        # Also set final doc.delivery_info["status"] = "delivered"
        self.doc.delivery_info["status"] = "delivered"
        # Now save doc
        self.ydm.save_document(self.doc)

        self._logger.info(
            f"[{self.project_id}] New delivery entry added: {new_delivery_data}"
        )

    def fetch_project_contact_emails(self):
        """
        Fetch PI, contact, and bioinformatician emails from user_info in the Yggdrasil doc.

        Returns:
            tuple: (pi_email, contact_email, bioinfo_email)
        """
        user_info = self.doc.user_info or {}
        pi_email = user_info.get("pi", {}).get("email", "")
        contact_email = user_info.get("owner", {}).get("email", "")
        bioinfo_email = user_info.get("bioinformatician", {}).get("email", "")
        return pi_email, contact_email, bioinfo_email

    async def run_cmd(self, cmd):
        """
        Run a shell command asynchronously. Logs output and returns exit code.
        """
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                self._logger.error(
                    f"Command failed: {' '.join(cmd)}\nStdout: {stdout.decode()}\nStderr:{stderr.decode()}"
                )
            else:
                self._logger.debug(f"Command success: {' '.join(cmd)}")
            return process.returncode
        except Exception as e:
            self._logger.error(
                f"Error running command {' '.join(cmd)}: {e}", exc_info=True
            )
            return None

    def update_delivery_info(self, project_id: str, updates: dict):
        """
        Update delivery_info in the Yggdrasil doc and save it.
        """
        doc = self.ydm.get_document_by_project_id(project_id)
        if not doc:
            self._logger.error(
                f"Cannot update delivery info, project {project_id} not found in yggdrasil DB."
            )
            return
        doc_delivery_info = doc.get("delivery_info", {})
        doc_delivery_info.update(updates)
        doc["delivery_info"] = doc_delivery_info
        from lib.couchdb.yggdrasil_document import YggdrasilDocument

        ygg_doc = YggdrasilDocument.from_dict(doc)
        self.ydm.save_document(ygg_doc)
        self._logger.info(f"Updated delivery_info for {project_id} with {updates}.")
        self._logger.info(f"Updated delivery_info for {project_id} with {updates}.")
