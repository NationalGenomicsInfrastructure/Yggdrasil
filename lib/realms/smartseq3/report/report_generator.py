import datetime
import json
import logging

from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (  # , Image
    ListFlowable,
    ListItem,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
)

from lib.core_utils.logging_utils import custom_logger
from lib.realms.smartseq3.report.utils.report_utils import (
    add_figures,
    check_high_nan_percentage,
    get_image,
)
from lib.realms.smartseq3.report.utils.ss3_data_collector import SS3DataCollector
from lib.realms.smartseq3.report.utils.ss3_figure_plotter import SS3FigurePlotter

logger = custom_logger(__name__)


class Smartseq3ReportGenerator:
    """
    Generates reports for SmartSeq3 samples, including statistics collection,
    graph creation, and PDF report rendering.

    Attributes:
        sample (object): The sample object containing sample data.
        style (StyleSheet1): The stylesheet used for formatting the report.
        file_handler (SampleFileHandler): Handles file operations for the sample.
        data_collector (SS3DataCollector): Collects data for the report.
    """

    def __init__(self, sample, logger: logging.Logger | None = None):
        """
        Initialize the Smartseq3ReportGenerator with sample information.

        Args:
            sample (object): The sample object containing sample data.
        """
        self._logger = logger or custom_logger(f"{__name__}.{type(self).__name__}")
        self.sample = sample

        self.style = self._create_report_style()

        # Get the zUMIsOutputHandler and create 'plots' folder
        self.file_handler = self.sample.file_handler
        # self.file_handler.create_plots_folder()

        # Initialize SS3DataCollector
        self.data_collector = SS3DataCollector(self.file_handler, self.sample)

    def _create_report_style(self):
        """
        Create and return the stylesheet for the report.

        Returns:
            StyleSheet1: The stylesheet used for formatting the report.
        """
        style = getSampleStyleSheet()
        style.add(ParagraphStyle(name="Justify", alignment=TA_JUSTIFY))

        return style

    def collect_stats(self):
        """
        Collect statistics for the report.

        Returns:
            bool: True if statistics are collected successfully, False otherwise.
        """
        self.stats = self.data_collector.collect_stats()

        if self.stats is None or self.stats.empty:
            self._logger.error("No statistics found. Check manually why that is.")
            return False
        else:
            self._logger.info("Statistics collected.")
            # self._logger.debug(self.stats)

        result = check_high_nan_percentage(self.stats, 10)
        if result:
            self._logger.warning(
                f"High number of NaNs detected ({result}%). Double check the given Barcode Set."
            )
            return False

        return True

    def create_graphs(self):
        """
        Create graphs for the report.

        Returns:
            bool: True if graphs are created successfully, False otherwise.
        """
        plotter = SS3FigurePlotter(
            self.sample.id, self.stats, self.file_handler.plots_dir
        )  # TODO: fix class to handle absense of out_dir
        self.biv_plot = plotter.create_bivariate_plate_map(
            "readspercell",
            "genecounts",
            "reads/cell",
            "Number of Genes",
            return_fig=True,
        )
        self.rvf_plot = plotter.reads_vs_frags(return_fig=True)
        self.uvc_plot = plotter.umi_tagged_vs_count(return_fig=True)

        if self.biv_plot and self.rvf_plot and self.uvc_plot:
            self._logger.info("Graphs created.")
            return True
        else:
            self._logger.error("Failed to create graphs.")
            return False

    def render(self, format="PDF"):
        """
        Render the report in the specified format.

        Args:
            format (str): The format of the report ('PDF' or 'HTML').

        Returns:
            None
        """
        if format.upper() == "PDF":
            return self._render_pdf()
        elif format.upper() == "HTML":
            return self._render_html()
        else:
            raise ValueError("Unsupported format. Choose 'PDF' or 'HTML'.")

    # TODO: Probably there's a better place for this path (use config_loader?)
    def _fetch_settings(self):
        """
        Fetch report settings from a JSON file.

        Returns:
            dict: The report settings.
        """
        with open("lib/realms/smartseq3/report/report_settings.json") as f:
            return json.load(f)

    def _prepare_overview_data(self):
        """
        Prepare overview data for the report.

        Returns:
            list: A list of lists containing overview data.
        """

        self.meta = self.data_collector.collect_meta(self.stats)

        # Prepare data for the overview section
        return [
            ["Project ID", self.sample.project_info["project_name"].replace("__", ".")],
            ["Plate ID", self.sample.metadata["plate"]],
            ["Barcode Set", self.sample.metadata.get("barcode", "--")],
            # ['Illumina Reagent kit', self.sample.project_info['sequencing_setup']],
            ["Flowcell ID", self.sample.flowcell_id.split("_")[-1][1:]],
            ["Genome", self.sample.project_info.get("ref_genome", None)],
            ["zUMIs version", self.meta.get("zUMIs_version", None)],
            ["Total number of sequenced reads", self.meta.get("total_reads", 0)],
            ["Filtered number of reads", self.meta.get("filtered_reads", 0)],
            ["Average sequence depth per cell", self.meta.get("avg_readspercell", 0)],
        ]

    def _render_html(self):
        """
        Render the report in HTML format.

        Returns:
            None
        """
        # TODO: Specific rendering for HTML
        pass

    def _render_pdf(self):
        """
        Render the report in PDF format.

        Returns:
            None
        """
        settings = self._fetch_settings()
        overview_data = self._prepare_overview_data()

        doc = SimpleDocTemplate(
            str(self.file_handler.report_fpath),
            pagesize=A4,
            topMargin=72,
            rightMargin=72,
            leftMargin=72,
            bottomMargin=18,
        )

        report_elements = self._build_report_elements(settings, overview_data)
        doc.build(report_elements)
        self._logger.info(f"Created Report at: {self.file_handler.report_fpath}")

    def _build_report_elements(self, settings, overview_data):
        """
        Build the elements of the report.

        Args:
            settings (dict): The report settings.
            overview_data (list): The overview data for the report.

        Returns:
            list: A list of report elements.
        """
        report_elements = []
        report_elements.extend(self._add_header(settings))
        report_elements.extend(self._add_body(settings, overview_data))
        report_elements.extend(self._add_graphs(settings))
        return report_elements

    def _add_header(self, settings):
        """
        Add the header section to the report.

        Args:
            settings (dict): The report settings.

        Returns:
            list: A list of header elements.
        """
        header_elements = []
        # Add logo
        header_elements.append(
            get_image(f"{settings['logo']}", 8 * cm, hAlign="CENTER")
        )
        header_elements.append(Spacer(1, 30))

        # Add title
        title = f'<font style = "font-family:Lato" size=12><b>{settings["title"]}</b></font>'
        header_elements.append(Paragraph(title, self.style["Title"]))
        header_elements.append(Spacer(1, 5))

        # Add address
        for part in settings["address"].split("\n"):
            text = f'<font style = "font-family:Lora" size=10>{part}</font>'
            header_elements.append(Paragraph(text, self.style["Normal"]))
        header_elements.append(Spacer(1, 5))

        # Add date
        date = datetime.datetime.now().date()
        date = f'<font style = "font-family:Lora" size=10>{date}</font>'
        header_elements.append(Paragraph(date, self.style["Normal"]))
        header_elements.append(Spacer(1, 20))

        return header_elements

    def _add_body(self, settings, overview_data):
        """
        Add the body section to the report.

        Args:
            settings (dict): The report settings.
            overview_data (list): The overview data for the report.

        Returns:
            list: A list of body elements.
        """
        body_elements = []

        # Add ending statements
        text = f'<font style = "font-family:Lora" size=10>{settings["end_statement1"]}<br/><br/><b>{settings["awknowledgement"]}</b><br/><br/>{settings["end_statement2"]}</font>'
        body_elements.append(Paragraph(text, self.style["Justify"]))
        body_elements.append(Spacer(1, 20))

        # Add references
        text = '<font style = "font-family:Lato" size=12><b>References</b><br/></font>'
        body_elements.append(Paragraph(text, self.style["Justify"]))
        body_elements.append(Spacer(1, 3))
        body_elements.append(
            ListFlowable(
                [
                    ListItem(
                        Paragraph(
                            f'<font style = "font-family:Lora" size=10>{settings["ref1"]}</font>',
                            self.style["Normal"],
                        ),
                        spaceBefore=5,
                    ),
                    ListItem(
                        Paragraph(
                            f'<font style = "font-family:Lora" size=10>{settings["ref2"]}</font>',
                            self.style["Normal"],
                        ),
                        spaceBefore=5,
                    ),
                ],
                bulletType="bullet",
                leftIndent=10,
            )
        )

        body_elements.append(PageBreak())

        # Add overview section
        text = '<font style = "font-family:Lato" size=12><b>Overview of single-cell transcriptome data</b><br/><br/></font>'
        body_elements.append(Paragraph(text, self.style["Justify"]))
        table = Table(overview_data, hAlign="LEFT")
        body_elements.append(table)
        body_elements.append(Spacer(1, 30))

        # Add processing description
        text = '<font style = "font-family:Lato" size=12><b>Description of sample and data processing</b><br/></font>'
        body_elements.append(Paragraph(text, self.style["Justify"]))
        body_elements.append(Spacer(1, 15))

        # Add description
        text = f'<font style = "font-family:Lora" size=10>{settings["process_descr1"]}<a href={settings["pio_link"]}>(link). </a>{settings["process_descr1_cont"]}</font>'
        body_elements.append(Paragraph(text, self.style["Justify"]))
        body_elements.append(Spacer(1, 10))

        # Add description
        text = f'<font style = "font-family:Lora" size=10>{settings["process_descr2"]}</font>'
        body_elements.append(Paragraph(text, self.style["Justify"]))
        body_elements.append(Spacer(1, 10))

        # Add NGI project summary
        text = f'<font style="font-family:Lora" size=10>{self.sample.sample_data.get("project_summary", "Find more regarding the sequencing information in the project summary report.")}</font>'
        body_elements.append(Paragraph(text, self.style["Justify"]))
        body_elements.append(Spacer(1, 10))

        # Add description
        text = f'<font style = "font-family:Lora" size=10>{settings["process_descr3"]}</font>'
        body_elements.append(Paragraph(text, self.style["Justify"]))
        body_elements.append(Spacer(1, 10))

        body_elements.append(Spacer(1, 10))

        # Add data files description
        text = '<font style = "font-family:Lato" size=12><b>Description of data files delivered</b><br/></font>'
        body_elements.append(Paragraph(text, self.style["Justify"]))
        body_elements.append(Spacer(1, 15))

        text = (
            f'<font style = "font-family:Lora" size=10>{settings["data_descr1"]}</font>'
        )
        body_elements.append(Paragraph(text, self.style["Justify"]))
        body_elements.append(Spacer(1, 10))

        text = (
            f'<font style = "font-family:Lora" size=10>{settings["data_descr2"]}</font>'
        )
        body_elements.append(Paragraph(text, self.style["Justify"]))
        body_elements.append(Spacer(1, 10))

        parts = settings["data_descr3"].split("\n")
        text = f'<font style = "font-family:Lora" size=10>{parts[0]}</font>'
        body_elements.append(Paragraph(text, self.style["Normal"]))

        body_elements.append(
            ListFlowable(
                [
                    ListItem(
                        Paragraph(
                            f'<font style = "font-family:Lora" size=10>{parts[1]}</font>',
                            self.style["Normal"],
                        ),
                        spaceBefore=5,
                        leftIndent=35,
                    ),
                    ListItem(
                        Paragraph(
                            f'<font style = "font-family:Lora" size=10>{parts[2]}</font>',
                            self.style["Normal"],
                        ),
                        spaceBefore=5,
                        leftIndent=35,
                    ),
                ],
                bulletType="bullet",
                leftIndent=10,
            )
        )

        body_elements.append(PageBreak())

        return body_elements

    def _add_graphs(self, settings):
        """
        Add the graphs section to the report.

        Args:
            settings (dict): The report settings.

        Returns:
            list: A list of graph elements.
        """
        graph_elements = []

        # Add graphs description
        text = '<font style = "font-family:Lato" size=12><b>QC information on the single cell data</b><br/></font>'
        graph_elements.append(Paragraph(text, self.style["Justify"]))
        graph_elements.append(Spacer(1, 15))

        text = (
            f'<font style = "font-family:Lora" size=10>{settings["fig_descr"]}</font>'
        )
        graph_elements.append(Paragraph(text, self.style["Justify"]))
        graph_elements.append(Spacer(1, 30))

        # Gather figure information
        fig_info = {
            "fig1": {
                "size": [800, 18],
                "title": "Figure 1: Features summary",
                "source": self.file_handler.features_plot_fpath,
                "legend": settings["fig1"],
            },
            "fig2": {
                "size": [600, 16],
                "title": "Figure 2: Number of sequenced reads and genes per well",
                "source": self.biv_plot,
                "legend": settings["fig2"],
            },
            "fig3": {
                "size": [500, 15],
                "title": "Figure 3: Proportion of UMI fragments and total number of sequenced reads per cell",
                "source": self.rvf_plot,
                "legend": settings["fig3"],
            },
            "fig4": {
                "size": [500, 15],
                "title": "Figure 4: Number of UMIs vs number of genes",
                "source": self.uvc_plot,
                "legend": settings["fig4"],
            },
        }

        graph_elements = add_figures(graph_elements, self.style, fig_info)

        return graph_elements
