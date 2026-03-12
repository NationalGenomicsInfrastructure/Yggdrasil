import logging
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd

from lib.core_utils.logging_utils import custom_logger
from lib.realms.smartseq3.report.utils.bivariate_plate_map import BivariatePlateMap

logger = custom_logger(__name__)


class SS3FigurePlotter:
    def __init__(
        self,
        platename,
        data,
        outdir,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the SS3Plotter with necessary data and output configurations.

        Args:
            data (pd.DataFrame): The processed data for plotting.
            outdir (str): Directory path where the plots will be saved.
            platename (str): Name of the plate or sample used in plot titles or filenames.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.data = data
        self.outdir = outdir
        self.platename = platename

    def create_bivariate_plate_map(
        self,
        color_title="",
        size_title="",
        log_color=True,
        fig_num="Fig2",
        return_fig=False,
    ):
        """
        Creates a bivariate plate map plot.

        Args:
            color_title (str, optional): Title for the color legend. Default is an empty string.
            size_title (str, optional): Title for the size legend. Default is an empty string.
            log_color (bool, optional): Whether to apply logarithmic scale to color values. Default is True.
            fig_num (str, optional): Figure number for naming the output file. Default is 'Fig2'.
            return_fig (bool, optional): Whether to return the figure object. Default is False.

        Returns:
            BytesIO or None: BytesIO object containing the figure if return_fig is True, otherwise None.
        """
        # Process DataFrame to fit BivariatePlateMap requirements
        plot_data = pd.DataFrame()
        plot_data["readspercell"] = self.data.loc[:, ("readspercell", "TotalReads")]
        plot_data["genecounts"] = self.data.loc[:, ("genecounts", "Intron+Exon")]
        plot_data["WellID"] = self.data.loc[:, ("bc_set", "WellID")]

        # print(f"Initial gc: {plot_data['genecounts'].min()}")
        # print(f"Initial rpc: {plot_data['readspercell'].min()}")

        # Create the BivariatePlateMap
        plate_map = BivariatePlateMap(
            data=plot_data,
            color_values="readspercell",
            size_values="genecounts",
            well_values="WellID",
            size_title=size_title,
            color_title=color_title,
            log_color=log_color,
            output_dir=self.outdir,
            fig_name=f"{self.platename}_bivariate_plate_map.pdf",
        )

        # Generate the plot
        plot = plate_map.generate_plot()

        if plot:
            self._logger.info(f"Generated bivariate plate map for {self.platename}.")
        else:
            self._logger.error(
                f"Failed to generate bivariate plate map for {self.platename}."
            )

        if return_fig:
            # Return the figure object
            buf = BytesIO()
            plot.savefig(buf, format="PNG")
            buf.seek(0)
            plate_map.save_plot()
            return buf
        else:
            # Save the plot to a file
            plt.savefig(f"{self.outdir}/{self.platename}_{fig_num}.pdf")
            plt.close()

    def reads_vs_frags(self, fig_num="Fig5", return_fig=False):
        """
        Generates and saves a scatter plot of sequenced reads versus UMI fragments.

        This plot visually represents the relationship between the number of sequenced reads and the
        percentage of UMI fragments for each well in the dataset.

        Args:
            fig_num (str): Figure number or identifier for the output file. Default is 'Fig5'.
            return_fig (bool): Whether to return the figure object instead of saving to a file. Default is False.
        """

        # Extract UMI tagged and non-tagged counts from the data
        umi_tagged = self.data.loc[:, ("BC_UMI_stats", "nUMItag")]
        non_tagged = self.data.loc[:, ("BC_UMI_stats", "nNontagged")]

        # Calculate the average sequenced reads and the percentage of UMI fragments
        seq_reads = (non_tagged + umi_tagged) / 2
        umi_frags = umi_tagged / (non_tagged + umi_tagged) * 100

        # Set up the plot
        plt.figure(figsize=(8, 5))
        plt.scatter(seq_reads, umi_frags, color="brown", alpha=0.5)
        plt.semilogx()

        # Set plot title and labels
        plt.title("")
        plt.xlabel("Sequenced Reads")
        plt.ylabel("% UMI Fragments")

        if return_fig:
            # Return the figure object
            buf = BytesIO()
            plt.savefig(buf, format="PNG")
            buf.seek(0)
            return buf
        else:
            # Save the plot to a file
            plt.savefig(f"{self.outdir}/{self.platename}_{fig_num}.pdf")
            plt.close()

    def umi_tagged_vs_count(self, fig_num="Fig6", return_fig=False):
        """
        Generates and saves a scatter plot of UMI genes detected versus UMI read counts.

        This plot visually represents the relationship between the number of genes detected
        and the UMI read counts for each well in the dataset.

        Args:
            fig_num (str): Figure number or identifier for the output file. Default is 'Fig6'.
            return_fig (bool): Whether to return the figure object instead of saving to a file. Default is False.
        """

        # Extract UMI genes detected and UMI read counts from the data
        umi_genes_dt = self.data.loc[:, ("Loom", "UMI_genes_detected")]
        umi_rcounts = self.data.loc[:, ("Loom", "UMI_read_counts")]

        # Set up the plot
        plt.figure(figsize=(8, 5))
        plt.scatter(umi_genes_dt, umi_rcounts, color="brown", alpha=0.5)

        # Set plot title and labels
        plt.xlabel("Number of genes detected - UMI reads")
        plt.ylabel("Number of UMIs")

        if return_fig:
            # Return the figure object
            buf = BytesIO()
            plt.savefig(buf, format="PNG")
            buf.seek(0)
            return buf
        else:
            # Save the plot to a file
            plt.savefig(f"{self.outdir}/{self.platename}_{fig_num}.pdf")
            plt.close()
            plt.close()
