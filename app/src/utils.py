from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .manifest import PipelineManifest


def display_pipelinemanifest(pipeline_manifest: PipelineManifest):
    """
    Displays a table of jobs organized by network, station, and campaign using rich.table.

    :param pipeline_manifest: The PipelineManifest object containing the jobs.
    :type pipeline_manifest: PipelineManifest
    """
    CONSOLE_WIDTH = 80
    console = Console()

    entry_message = Panel(
        "[bold cyan]Starting the Earthscope Seafloor Geodesy Processing Application[/bold cyan]",
        expand=False,
        border_style="blue",
        width=CONSOLE_WIDTH,
    )
    # Create a table
    jobtable = Table(title="Pipeline Jobs", show_lines=True,width=CONSOLE_WIDTH)

    # Add columns
    jobtable.add_column("Network", style="cyan", no_wrap=True)
    jobtable.add_column("Station", style="magenta", no_wrap=True)
    jobtable.add_column("Campaign", style="green", no_wrap=True)
    jobtable.add_column("Job Type", style="yellow", no_wrap=True)
    # Add rows for ingestion jobs
    for job in pipeline_manifest.ingestion_jobs:
        jobtable.add_row(
            job.network,
            job.station,
            job.campaign,
            "Ingestion"
        )

    # Add rows for processing jobs
    for job in pipeline_manifest.process_jobs:
        jobtable.add_row(
            job.network,
            job.station,
            job.campaign,
            "Processing",
        )

    # Add rows for download jobs
    for job in pipeline_manifest.download_jobs:
        jobtable.add_row(
            job.network, job.station, job.campaign, "Download"
        )
    
    # Add rows for garpos
    for job in pipeline_manifest.garpos_jobs:
        jobtable.add_row(
            job.network, job.station, job.campaign, "Garpos"
        )

    metatable = Table(title="Pipeline Metadata", show_lines=True, width=CONSOLE_WIDTH)
    metatable.add_column("Field", style="cyan", no_wrap=True)
    metatable.add_column("Value", style="magenta", no_wrap=True)
    metatable.add_row("Main Directory", str(pipeline_manifest.main_dir))
   
    # Print the entry message
    console.print(entry_message)
    console.print(metatable)
    # Print the table
    console.print(jobtable)

