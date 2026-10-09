import asyncio
import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from yggdrasil.watchers.seq_data_watcher import SeqDataWatcher, YggdrasilEvent


class TestSeqDataWatcher(unittest.TestCase):

    def setUp(self):
        # Prepare an event loop for each test to run async watchers
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # We'll capture the YggdrasilEvents here
        self.captured_events = []

        def on_event_callback(event: YggdrasilEvent):
            """Mock callback to store events for assertion."""
            self.captured_events.append(event)

        self.on_event_callback = on_event_callback

        # Create a logger with stdout or do silent
        logging.basicConfig(level=logging.DEBUG)

    def tearDown(self):
        # Safely close the event loop
        self.loop.close()

    def test_marker_file_detection(self):
        """
        End-to-end test that ensures the SeqDataWatcher notices when
        the required marker files are created in a subfolder.
        """

        # We'll create a temporary directory to simulate the instrument directory
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            # Our marker files; we want both to appear
            marker_files = {"RTAComplete.txt", "CopyComplete.txt"}

            # We'll watch for an "Illumina" instrument
            config = {
                "instrument_name": "Illumina",
                "directory_to_watch": str(tmp_path),
                "marker_files": marker_files,
            }

            # Instantiate the watcher
            watcher = SeqDataWatcher(
                on_event=self.on_event_callback,
                config=config,
                logger=logging.getLogger("TestSeqDataWatcher"),
                recursive=True,
            )

            async def test_flow():
                # Start the watcher
                start_task = asyncio.create_task(watcher.start())

                await asyncio.sleep(1.0)  # Give the watcher time to initialize

                # Create a subfolder to mimic a new flowcell directory
                flowcell_dir = tmp_path / "20250101_999_AABBCCTEST"
                flowcell_dir.mkdir(parents=True, exist_ok=True)

                # Sleep briefly so the folder creation event is processed (optional)
                await asyncio.sleep(0.5)

                # Place the first marker file
                (flowcell_dir / "RTAComplete.txt").write_text("Simulating RTAComplete")

                await asyncio.sleep(0.5)  # Let the watcher see the first file

                # Place the second marker file
                (flowcell_dir / "CopyComplete.txt").write_text(
                    "Simulating CopyComplete"
                )

                # Wait a bit for the watcher to detect the second file
                await asyncio.sleep(1.0)

                # Stop the watcher
                await watcher.stop()
                await start_task

            self.loop.run_until_complete(test_flow())

            # Now check that we received exactly one event, as all markers found
            self.assertEqual(
                len(self.captured_events), 1, "Should have exactly 1 event"
            )

            event = self.captured_events[0]
            self.assertEqual(event.event_type, "flowcell_ready")
            self.assertEqual(event.payload["instrument"], "Illumina")
            self.assertIn("subfolder", event.payload)
            self.assertIn(
                event.source,
                ["filesystem", "SeqDataWatcher"],
                f"Event source should be 'filesystem' or 'SeqDataWatcher', got '{event.source}'",
            )


if __name__ == "__main__":
    unittest.main()
