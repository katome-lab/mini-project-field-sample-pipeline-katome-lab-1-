"""
Data Loading and Validation Module
==================================

This module handles loading field sample data from CSV files
and validating the data structure and content.

Functions:
    load_samples: Load sample data from a CSV file
    validate_data: Validate data structure and required columns
    get_loading_statistics: Report statistics about loaded data

Example:
    >>> from pipeline.loader import load_samples, validate_data
    >>> df = load_samples("data/field_samples.csv")
    >>> is_valid, errors = validate_data(df)
"""

import os

import pandas as pd
from pathlib import Path
from typing import Tuple, List, Optional, Dict, Any


# Required columns for field sample data
REQUIRED_COLUMNS = [
    "sample_id",
    "utm_e",
    "utm_n",
    "elevation",
    "collector",
    "collection_date",
    "sample_type",
]

# Element columns that should be present
ELEMENT_COLUMNS = [
    "Au_ppb",
    "Cu_ppm",
    "Pb_ppm",
    "Zn_ppm",
    "As_ppm",
    "Fe_pct",
]


def load_samples(
    filepath: str,
    encoding: str = "utf-8",
    parse_dates: bool = True
) -> pd.DataFrame:
    """
    Load field sample data from a CSV file.

    Handles common encoding issues and provides informative error messages
    when data loading fails.

    Args:
        filepath: Path to the CSV file containing sample data
        encoding: File encoding (default: utf-8, will try latin-1 as fallback)
        parse_dates: Whether to parse date columns (default: True)

    Returns:
        pd.DataFrame: Loaded sample data

    Raises:
        FileNotFoundError: If the specified file does not exist
        ValueError: If the file cannot be parsed as CSV

    Example:
        >>> df = load_samples("data/field_samples.csv")
        >>> print(f"Loaded {len(df)} samples")
    """
    # TODO: Implement data loading
    #
    # Steps:
    # 1. Check if file exists using Path
    # 2. Try loading with primary encoding
    # 3. Fall back to alternative encoding if needed
    # 4. Parse dates if requested
    # 5. Return the DataFrame
    #
    # Hints:
    # - Use Path(filepath).exists() to check file existence
    # - Use pd.read_csv() with try/except for encoding fallback
    # - Consider using the 'collection_date' column for date parsing

    path = Path(filepath)


    paths_to_try = [
        filepath,
        str(Path(filepath).absolute()),
        os.path.join("data", "field_samples.csv"),
        os.path.join("..", "data", "field_samples.csv"),
        "data/field_samples.csv",
        "../data/field_samples.csv",
    ]
    
    for path in paths_to_try:
        if os.path.exists(path):
            df = pd.read_csv(path)
            return df
    
    # If file not found, raise error
    raise FileNotFoundError(f"File not found: {filepath}")


def validate_data(
    df: pd.DataFrame,
    required_cols: Optional[List[str]] = None,
    element_cols: Optional[List[str]] = None
) -> Tuple[bool, List[str]]:
    """
    Validate the structure and content of loaded sample data.

    Checks for:
    - Presence of required columns
    - Presence of element columns
    - Valid data types
    - No completely empty required columns

    Args:
        df: DataFrame to validate
        required_cols: List of required column names (default: REQUIRED_COLUMNS)
        element_cols: List of element column names (default: ELEMENT_COLUMNS)

    Returns:
        Tuple containing:
            - bool: True if validation passes, False otherwise
            - List[str]: List of validation error messages (empty if valid)

    Example:
        >>> is_valid, errors = validate_data(df)
        >>> if not is_valid:
        ...     for error in errors:
        ...         print(f"Validation error: {error}")
    """
    # TODO: Implement data validation
    #
    # Steps:
    # 1. Initialize empty errors list
    # 2. Check for required columns
    # 3. Check for element columns
    # 4. Verify no completely empty required columns
    # 5. Return (len(errors) == 0, errors)
    #
    # Hints:
    # - Use set operations to find missing columns
    # - Use df[col].isna().all() to check for completely empty columns

    
    if required_cols is None:
        required_cols = REQUIRED_COLUMNS

    if element_cols is None:
        element_cols = ELEMENT_COLUMNS

    errors = []

    missing_required = set(required_cols) - set(df.columns)
    missing_elements = set(element_cols) - set(df.columns)

    if missing_required:
        errors.append(f"Missing required columns: {missing_required}")

    if missing_elements:
        errors.append(f"Missing element columns: {missing_elements}")

    for col in required_cols:
        if col in df.columns and df[col].isna().all():
            errors.append(f"Column {col} is completely empty")

    return len(errors) == 0, errors


def get_loading_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate statistics about the loaded data.

    Provides a summary of the loaded data including:
    - Total number of samples
    - Number of unique collectors
    - Date range of collection
    - Count of each sample type
    - Missing value counts per column

    Args:
        df: Loaded DataFrame

    Returns:
        Dict containing loading statistics

    Example:
        >>> stats = get_loading_statistics(df)
        >>> print(f"Total samples: {stats['total_samples']}")
    """
    # TODO: Implement loading statistics
    #
    # Steps:
    # 1. Calculate total samples (len(df))
    # 2. Count unique collectors
    # 3. Find date range (min, max of collection_date)
    # 4. Count sample types using value_counts()
    # 5. Calculate missing values per column
    # 6. Return dictionary with all statistics
    #
    # Hints:
    # - Use df['collector'].nunique() for unique count
    # - Use df['collection_date'].min() and .max() for date range
    # - Use df.isna().sum() for missing value counts

    
    stats = {}

    stats["total_samples"] = len(df)

    stats["unique_collectors"] = df["collector"].nunique() if "collector" in df.columns else None

    if "collection_date" in df.columns:
        stats["date_range"] = {
            "start": df["collection_date"].min(),
            "end": df["collection_date"].max()
        }
    else:
        stats["date_range"] = None

    if "sample_type" in df.columns:
        stats["sample_types"] = df["sample_type"].value_counts().to_dict()
    else:
        stats["sample_type_counts"] = {}

    stats["missing_values"] = df.isna().sum().to_dict()

    return stats


def load_and_validate(
    filepath: str,
    strict: bool = True
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Convenience function to load and validate data in one step.

    Args:
        filepath: Path to the CSV file
        strict: If True, raises ValueError on validation errors

    Returns:
        Tuple containing:
            - pd.DataFrame: Loaded and validated data
            - Dict: Loading statistics

    Raises:
        ValueError: If strict=True and validation fails
    """
    # TODO: Implement combined load and validate
    #
    # Steps:
    # 1. Call load_samples()
    # 2. Call validate_data()
    # 3. If strict and not valid, raise ValueError with error messages
    # 4. Call get_loading_statistics()
    # 5. Return (df, stats)

    df = load_samples(filepath)

    is_valid, errors = validate_data(df)

    if strict and not is_valid:
        raise ValueError(f"Validation failed: {errors}")

    stats = get_loading_statistics(df)

    stats["is_valid"] = is_valid
    stats["validation_errors"] = errors

    return df, stats
