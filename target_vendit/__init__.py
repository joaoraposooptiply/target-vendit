"""Target Vendit - Singer target for Vendit API."""

from .target import TargetVendit

__version__ = "0.1.0"

def main():
    """Main entry point for the target."""
    TargetVendit.cli()
