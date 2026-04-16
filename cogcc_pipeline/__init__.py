"""COGCC production data pipeline package."""

from cogcc_pipeline.acquire import (
    build_download_targets,
    download_file,
    is_valid_download,
    run_acquire,
)
from cogcc_pipeline.ingest import enforce_schema, load_data_dictionary, map_dtype, run_ingest
from cogcc_pipeline.transform import (
    _add_derived_columns,
    _clean_production_volumes,
    _deduplicate,
    _normalize_operator_names,
    check_well_completeness,
    run_transform,
)
from cogcc_pipeline.features import (
    _add_cumulative_features,
    _add_decline_rates,
    _add_encoded_features,
    _add_lag_features,
    _add_ratio_features,
    _add_rolling_features,
    run_features,
)
from cogcc_pipeline.utils import get_partition_count, load_config, setup_logging

__all__ = [
    # acquire
    "build_download_targets",
    "download_file",
    "is_valid_download",
    "run_acquire",
    # ingest
    "enforce_schema",
    "load_data_dictionary",
    "map_dtype",
    "run_ingest",
    # transform
    "_add_derived_columns",
    "_clean_production_volumes",
    "_deduplicate",
    "_normalize_operator_names",
    "check_well_completeness",
    "run_transform",
    # features
    "_add_cumulative_features",
    "_add_decline_rates",
    "_add_encoded_features",
    "_add_lag_features",
    "_add_ratio_features",
    "_add_rolling_features",
    "run_features",
    # utils
    "get_partition_count",
    "load_config",
    "setup_logging",
]
