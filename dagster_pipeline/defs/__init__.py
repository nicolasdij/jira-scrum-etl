# `dg dev` resolves definitions via `<root_module>.defs`.
# Re-export from the main definitions module to satisfy that convention
# without duplicating any code.
from dagster_pipeline.definitions import defs

__all__ = ["defs"]
