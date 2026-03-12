#!/usr/bin/env python
import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from lib.core_utils.common import YggdrasilUtilities as Ygg

# from lib.core_utils.config_loader import configs as ygg_configs
from lib.core_utils.config_loader import ConfigLoader
from lib.core_utils.logging_utils import configure_logging
from lib.couchdb.project_db_manager import ProjectDBManager
from lib.couchdb.yggdrasil_db_manager import YggdrasilDBManager

# Call configure_logging to set up the logging environment
configure_logging(debug=True)

ygg_configs: Mapping[str, Any] = ConfigLoader().load_config("config.json")


# Define asynchronous functions for task handling
async def process_couchdb_changes():
    tasks = []
    pdm = ProjectDBManager()
    ydm = YggdrasilDBManager()

    while True:
        try:
            # Fetch data from CouchDB and call the appropriate module
            async for data, module_loc in pdm.fetch_changes():
                try:
                    # project_id = data.get("project_id")

                    # # Check if the project exists
                    # existing_document = ydm.check_project_exists(project_id)

                    # if existing_document is None:
                    #     projects_reference = data.get("_id")
                    #     method = data.get("details", {}).get(
                    #         "library_construction_method"
                    #     )

                    #     # Create a new project if it doesn't exist
                    #     ydm.create_project(project_id, projects_reference, method)
                    #     process_project = True
                    # else:
                    #     # If the project exists, check if it is completed
                    #     if existing_document.get("status") == "completed":
                    #         logging.info(
                    #             f"Project with ID {project_id} is already completed. Skipping further processing."
                    #         )
                    #         process_project = False
                    #     else:
                    #         logging.info(
                    #             f"Project with ID {project_id} is ongoing and will be processed."
                    #         )
                    #         process_project = True

                    # if process_project:
                    # Dynamically load the module
                    # module = Ygg.load_module(module_loc)
                    print(f">>> Module location: {module_loc}")
                    RealmClass = Ygg.load_realm_class(module_loc)

                    if RealmClass:
                        # Call the module's launch function
                        realm = RealmClass(data, ydm)
                        if realm.proceed:
                            task = asyncio.create_task(realm.launch())
                            tasks.append(task)
                            # print(f"Tasks ({realm.project_info['project_id']}): {tasks}")
                        else:
                            logging.info(
                                f"Skipping task creation due to missing required information. {data.get('project_id')}"
                            )
                    else:
                        logging.warning(
                            f"Failed to load module '{module_loc}' for '{data['details']['library_construction_method']}'."
                        )
                except Exception as e:
                    logging.warning(
                        f"Error while trying to load module: {e}", exc_info=True
                    )
                    logging.error(f"Data causing the error: {data}")

                # Limit the number of concurrent tasks if necessary
                # TODO: move to using ygg_configs["tasks_limit"] or similar
                tasks_limit = 2
                if len(tasks) >= tasks_limit:
                    # Wait for all tasks to complete
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    # Check for exceptions
                    for result in results:
                        if isinstance(result, Exception):
                            logging.error(
                                f"Task raised an exception: {result}",
                                exc_info=True,
                            )
                    tasks = []

                # Sleep to avoid excessive polling
                print("Sleeping in async for loop...")
                await asyncio.sleep(ygg_configs["couchdb_poll_interval"])

            # If an HPC job is required, submit it asynchronously
            # await submit_hpc_job(data)
            # except Exception as e:
            #     logging.error(f"An error occurred: {e}", exc_info=True)
            # logging.error(f"Data causing the error: {data}")

            # After the loop, wait for any remaining tasks to complete
            # |<-- if goes one indentation back if except block above is uncommented NOTE
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logging.error(
                            f"Task raised an exception: {result}",
                            exc_info=True,
                        )
                tasks = []

        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)

        # Sleep to avoid excessive polling
        print("Sleeping in while loop...")
        await asyncio.sleep(ygg_configs["couchdb_poll_interval"])


# Main daemon loop
async def main():
    """
    Main daemon loop.
    """
    # config = load_json_config()

    # Start the asynchronous coroutine for processing CouchDB changes
    couchdb_task = process_couchdb_changes()

    # tasks = asyncio.all_tasks()
    # for task in tasks:
    #     print(f"> {task.get_name()}, {task.get_coro()}")

    # Wait for both tasks to complete
    await asyncio.gather(couchdb_task)


if __name__ == "__main__":
    asyncio.run(main())
