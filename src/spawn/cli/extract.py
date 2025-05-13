"""
Extract command for SPAwn CLI.

Handles metadata extraction from files.
"""
from typing import Optional
from pathlib import Path
import click

@click.command(name="extract")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--save-json/--no-save-json",
    default=False,
    help="Whether to save metadata as JSON file",
)
@click.option(
    "--json-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory to save JSON metadata file in",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Path to save JSON metadata file (overrides --json-dir)",
)
def extract_file_metadata(file: Path, save_json: bool, json_dir: Optional[Path], output: Optional[Path]) -> None:
    """
    Extract metadata from a single file.

    FILE is the path to the file to extract metadata from.
    """
    from spawn.extractors.metadata import extract_metadata, save_metadata_to_json
    import json
    metadata = extract_metadata(file)
    print(json.dumps(metadata, indent=2, default=str))
    if save_json or output:
        if output:
            output_path = output
            output_dir = output.parent
            output_dir.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(metadata, f, indent=2, default=str)
            print(f"\nMetadata saved to: {output_path}")
        else:
            try:
                json_path = save_metadata_to_json(file, metadata, json_dir)
                print(f"\nMetadata saved to: {json_path}")
            except Exception as e:
                print(f"\nError saving metadata to JSON: {e}") 