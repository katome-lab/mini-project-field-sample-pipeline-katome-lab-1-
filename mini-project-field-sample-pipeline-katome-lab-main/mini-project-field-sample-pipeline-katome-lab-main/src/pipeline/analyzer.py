"""
Statistical Analysis Module
===========================

This module performs statistical analysis on cleaned field sample data,
including descriptive statistics, group-by analysis, and correlation studies.

Functions:
    calculate_statistics: Calculate descriptive statistics for elements
    group_statistics: Calculate statistics grouped by category
    correlation_analysis: Compute correlation matrix for elements
    identify_significant_correlations: Find statistically significant relationships

Example:
    >>> from pipeline.analyzer import calculate_statistics, correlation_analysis
    >>> stats = calculate_statistics(df, elements=["Au_ppb", "Cu_ppm"])
    >>> correlations = correlation_analysis(df)
"""
import itertools
import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any, Tuple
from scipy import stats as scipy_stats


# Default element columns for analysis
DEFAULT_ELEMENTS = ["Au_ppb", "Cu_ppm", "Pb_ppm", "Zn_ppm", "As_ppm", "Fe_pct"]


def calculate_statistics(
    df: pd.DataFrame,
    elements: Optional[List[str]] = None,
    percentiles: Optional[List[float]] = None
) -> pd.DataFrame:
    """
    Calculate comprehensive descriptive statistics for element columns.

    Computes:
    - Count, mean, std, min, max
    - Percentiles (default: 25%, 50%, 75%, 90%, 95%)
    - Skewness and kurtosis
    - Coefficient of variation

    Args:
        df: Input DataFrame with element data
        elements: List of element columns to analyze (default: DEFAULT_ELEMENTS)
        percentiles: List of percentiles to calculate (default: [25, 50, 75, 90, 95])

    Returns:
        pd.DataFrame: Statistics table with elements as columns

    Example:
        >>> stats = calculate_statistics(df, elements=["Au_ppb", "Cu_ppm"])
        >>> print(stats.loc["mean"])  # Print mean for each element
    """
    # TODO: Implement descriptive statistics calculation
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. If percentiles is None, use [25, 50, 75, 90, 95]
    # 3. Create empty dictionary for results
    # 4. For each element:
    #    a. Calculate count, mean, std, min, max
    #    b. Calculate percentiles using np.percentile()
    #    c. Calculate skewness using scipy_stats.skew()
    #    d. Calculate kurtosis using scipy_stats.kurtosis()
    #    e. Calculate coefficient of variation (std/mean * 100)
    # 5. Create DataFrame from results dictionary
    # 6. Return transposed DataFrame (elements as columns)
    #
    # Hints:
    # - Use df[element].dropna() before calculations
    # - Use scipy_stats.skew() and scipy_stats.kurtosis()
    # - CV = (std / mean) * 100

    if elements is None:
        elements = DEFAULT_ELEMENTS

    if percentiles is None:
        percentiles = [25, 50, 75, 90, 95]

    elements = [e for e in elements if e in df.columns]

    results = {}

    for element in elements:

        data = df[element].dropna()

        if len(data) == 0:
            continue

        stats = {
            "count": data.count(),
            "mean": data.mean(),
            "std": data.std(),
            "min": data.min(),
            "max": data.max(),
            "skew": scipy_stats.skew(data),
            "kurtosis": scipy_stats.kurtosis(data)
        }

        for p in percentiles:
            stats[f"p{p}"] = np.percentile(data, p)

        stats["cv_pct"] = (stats["std"] / stats["mean"]) * 100 if stats["mean"] != 0 else np.nan

        results[element] = stats

    return pd.DataFrame(results)


def group_statistics(
    df: pd.DataFrame,
    group_by: str,
    elements: Optional[List[str]] = None,
    agg_functions: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Calculate statistics grouped by a categorical column.

    Args:
        df: Input DataFrame
        group_by: Column name to group by (e.g., "sample_type", "collector")
        elements: Element columns to analyze (default: DEFAULT_ELEMENTS)
        agg_functions: Aggregation functions to apply
                      (default: ["count", "mean", "std", "min", "max"])

    Returns:
        pd.DataFrame: Multi-level DataFrame with groups as rows,
                     elements and statistics as columns

    Example:
        >>> grouped = group_statistics(df, group_by="sample_type")
        >>> print(grouped.loc["soil"])  # Stats for soil samples
    """
    # TODO: Implement grouped statistics
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. If agg_functions is None, use ["count", "mean", "std", "min", "max"]
    # 3. Filter to only include elements that exist in DataFrame
    # 4. Use df.groupby(group_by)[elements].agg(agg_functions)
    # 5. Return the result
    #
    # Hints:
    # - Use list comprehension to filter elements: [e for e in elements if e in df.columns]
    # - The result will have MultiIndex columns (element, statistic)

    if elements is None:
        elements = DEFAULT_ELEMENTS

    if agg_functions is None:
        agg_functions = ["count", "mean", "std", "min", "max"]

    elements = [e for e in elements if e in df.columns]

    grouped = df.groupby(group_by)[elements].agg(agg_functions)

    return grouped


def correlation_analysis(
    df: pd.DataFrame,
    elements: Optional[List[str]] = None,
    method: str = "pearson"
) -> pd.DataFrame:
    """
    Compute correlation matrix between element columns.

    Args:
        df: Input DataFrame
        elements: Element columns to include (default: DEFAULT_ELEMENTS)
        method: Correlation method - "pearson", "spearman", or "kendall"

    Returns:
        pd.DataFrame: Correlation matrix

    Example:
        >>> corr_matrix = correlation_analysis(df, method="spearman")
        >>> print(corr_matrix.loc["Au_ppb", "Cu_ppm"])  # Au-Cu correlation
    """
    # TODO: Implement correlation analysis
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. Filter to elements that exist in DataFrame
    # 3. Use df[elements].corr(method=method)
    # 4. Return correlation matrix
    #
    # Hints:
    # - pandas corr() method supports "pearson", "spearman", "kendall"

    if elements is None:
        elements = DEFAULT_ELEMENTS

    elements = [e for e in elements if e in df.columns]

    if len(elements) < 2:
        return pd.DataFrame()

    return df[elements].corr(method=method)



def identify_significant_correlations(
    df: pd.DataFrame,
    elements: Optional[List[str]] = None,
    threshold: float = 0.5,
    p_value_threshold: float = 0.05
) -> List[Dict[str, Any]]:
    """
    Identify statistically significant correlations between elements.

    Args:
        df: Input DataFrame
        elements: Element columns to analyze (default: DEFAULT_ELEMENTS)
        threshold: Minimum absolute correlation to report (default: 0.5)
        p_value_threshold: Maximum p-value for significance (default: 0.05)

    Returns:
        List of dictionaries containing:
            - element_1: First element name
            - element_2: Second element name
            - correlation: Correlation coefficient
            - p_value: Statistical p-value
            - significant: Whether p-value < threshold

    Example:
        >>> significant = identify_significant_correlations(df, threshold=0.6)
        >>> for corr in significant:
        ...     print(f"{corr['element_1']} - {corr['element_2']}: r={corr['correlation']:.3f}")
    """
    # TODO: Implement significant correlation identification
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. Filter to elements that exist in DataFrame
    # 3. Initialize empty results list
    # 4. For each unique pair of elements (avoid duplicates):
    #    a. Use scipy_stats.pearsonr() to get correlation and p-value
    #    b. If abs(correlation) >= threshold:
    #       - Add to results list with element names, r, p-value, significance
    # 5. Sort results by absolute correlation (descending)
    # 6. Return results list
    #
    # Hints:
    # - Use itertools.combinations(elements, 2) for unique pairs
    # - scipy_stats.pearsonr() returns (correlation, p_value)
    # - Handle NaN values by dropping them before correlation calculation

    if elements is None:
        elements = DEFAULT_ELEMENTS

    elements = [e for e in elements if e in df.columns]

    results = []

    for a, b in itertools.combinations(elements, 2):

        pair = df[[a, b]].dropna()

        if len(pair) < 3:
            continue

        try:
            r, p = scipy_stats.pearsonr(pair[a], pair[b])
        except Exception:
            continue

        if np.isnan(r) or np.isnan(p):
            continue

        if abs(r) >= threshold:
            results.append({
                "element_1": a,
                "element_2": b,
                "correlation": r,
                "p_value": p,
                "significant": p < p_value_threshold
            })

    results.sort(key=lambda x: abs(x["correlation"]), reverse=True)

    return results




def analyze_by_spatial_region(
    df: pd.DataFrame,
    n_regions: int = 4,
    elements: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Divide samples into spatial regions and analyze each region.

    Divides the study area into a grid based on UTM coordinates
    and calculates statistics for each region.

    Args:
        df: Input DataFrame with utm_e and utm_n columns
        n_regions: Number of regions per axis (creates n_regions^2 grid cells)
        elements: Elements to analyze (default: DEFAULT_ELEMENTS)

    Returns:
        Dictionary with region IDs as keys and statistics DataFrames as values

    Example:
        >>> regional_stats = analyze_by_spatial_region(df, n_regions=3)
        >>> print(regional_stats["region_0_1"])  # Stats for region at row 0, col 1
    """
    # TODO: Implement spatial region analysis
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. Calculate UTM coordinate ranges
    # 3. Create bins for easting and northing
    # 4. Assign each sample to a region based on its coordinates
    # 5. For each region with data:
    #    a. Calculate statistics using calculate_statistics()
    #    b. Store in results dictionary
    # 6. Return dictionary of regional statistics
    #
    # Hints:
    # - Use pd.cut() to bin coordinates
    # - Region ID could be f"region_{row}_{col}"

    if elements is None:
        elements = DEFAULT_ELEMENTS

    elements = [e for e in elements if e in df.columns]

    if "utm_e" not in df.columns or "utm_n" not in df.columns:
        raise ValueError("DataFrame must contain 'utm_e' and 'utm_n' columns")

    e_min, e_max = df["utm_e"].min(), df["utm_e"].max()
    n_min, n_max = df["utm_n"].min(), df["utm_n"].max()

    e_bins = np.linspace(e_min, e_max, n_regions + 1)
    n_bins = np.linspace(n_min, n_max, n_regions + 1)

    df = df.copy()

    df["region_e"] = pd.cut(df["utm_e"], bins=e_bins, labels=False, include_lowest=True)
    df["region_n"] = pd.cut(df["utm_n"], bins=n_bins, labels=False, include_lowest=True)

    results = {}

    for (i, j), group in df.groupby(["region_n", "region_e"]):

        if group.empty:
            continue

        region_id = f"region_{i}_{j}"

        stats_df = calculate_statistics(group, elements=elements)

        results[region_id] = stats_df

    return results

def generate_analysis_summary(
    df: pd.DataFrame,
    elements: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Generate a comprehensive analysis summary.

    Combines all analysis functions into a single summary report.

    Args:
        df: Input DataFrame
        elements: Elements to analyze (default: DEFAULT_ELEMENTS)

    Returns:
        Dictionary containing:
            - descriptive_stats: Full statistics DataFrame
            - correlations: Correlation matrix
            - significant_correlations: List of significant correlations
            - sample_type_stats: Statistics by sample type
            - collector_stats: Statistics by collector

    Example:
        >>> summary = generate_analysis_summary(df)
        >>> print(summary["descriptive_stats"])
    """
    # TODO: Implement analysis summary generation
    #
    # Steps:
    # 1. Call calculate_statistics()
    # 2. Call correlation_analysis()
    # 3. Call identify_significant_correlations()
    # 4. Call group_statistics() for sample_type
    # 5. Call group_statistics() for collector
    # 6. Compile all results into summary dictionary
    # 7. Return summary
    summary = {}
    summary["descriptive_stats"] = calculate_statistics(df, elements=elements)
    summary["correlations"] = correlation_analysis(df, elements=elements)
    summary["significant_correlations"] = identify_significant_correlations(df, elements=elements)
    if "sample_type" in df.columns:
        summary["sample_type_stats"] = group_statistics(df, "sample_type", elements=elements)
    else:
        summary["sample_type_stats"] = pd.DataFrame()
    if "collector" in df.columns:
        summary["collector_stats"] = group_statistics(df, "collector", elements=elements)
    else:
        summary["collector_stats"] = pd.DataFrame()

    return summary"""
Statistical Analysis Module
===========================

This module performs statistical analysis on cleaned field sample data,
including descriptive statistics, group-by analysis, and correlation studies.

Functions:
    calculate_statistics: Calculate descriptive statistics for elements
    group_statistics: Calculate statistics grouped by category
    correlation_analysis: Compute correlation matrix for elements
    identify_significant_correlations: Find statistically significant relationships

Example:
    >>> from pipeline.analyzer import calculate_statistics, correlation_analysis
    >>> stats = calculate_statistics(df, elements=["Au_ppb", "Cu_ppm"])
    >>> correlations = correlation_analysis(df)
"""
import itertools
import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any, Tuple
from scipy import stats as scipy_stats


# Default element columns for analysis
DEFAULT_ELEMENTS = ["Au_ppb", "Cu_ppm", "Pb_ppm", "Zn_ppm", "As_ppm", "Fe_pct"]


def calculate_statistics(
    df: pd.DataFrame,
    elements: Optional[List[str]] = None,
    percentiles: Optional[List[float]] = None
) -> pd.DataFrame:
    """
    Calculate comprehensive descriptive statistics for element columns.

    Computes:
    - Count, mean, std, min, max
    - Percentiles (default: 25%, 50%, 75%, 90%, 95%)
    - Skewness and kurtosis
    - Coefficient of variation

    Args:
        df: Input DataFrame with element data
        elements: List of element columns to analyze (default: DEFAULT_ELEMENTS)
        percentiles: List of percentiles to calculate (default: [25, 50, 75, 90, 95])

    Returns:
        pd.DataFrame: Statistics table with elements as columns

    Example:
        >>> stats = calculate_statistics(df, elements=["Au_ppb", "Cu_ppm"])
        >>> print(stats.loc["mean"])  # Print mean for each element
    """
    # TODO: Implement descriptive statistics calculation
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. If percentiles is None, use [25, 50, 75, 90, 95]
    # 3. Create empty dictionary for results
    # 4. For each element:
    #    a. Calculate count, mean, std, min, max
    #    b. Calculate percentiles using np.percentile()
    #    c. Calculate skewness using scipy_stats.skew()
    #    d. Calculate kurtosis using scipy_stats.kurtosis()
    #    e. Calculate coefficient of variation (std/mean * 100)
    # 5. Create DataFrame from results dictionary
    # 6. Return transposed DataFrame (elements as columns)
    #
    # Hints:
    # - Use df[element].dropna() before calculations
    # - Use scipy_stats.skew() and scipy_stats.kurtosis()
    # - CV = (std / mean) * 100

    if elements is None:
        elements = DEFAULT_ELEMENTS

    if percentiles is None:
        percentiles = [25, 50, 75, 90, 95]

    elements = [e for e in elements if e in df.columns]

    results = {}

    for element in elements:

        data = df[element].dropna()

        if len(data) == 0:
            continue

        stats = {
            "count": data.count(),
            "mean": data.mean(),
            "std": data.std(),
            "min": data.min(),
            "max": data.max(),
            "skew": scipy_stats.skew(data),
            "kurtosis": scipy_stats.kurtosis(data)
        }

        for p in percentiles:
            stats[f"p{p}"] = np.percentile(data, p)

        stats["cv_pct"] = (stats["std"] / stats["mean"]) * 100 if stats["mean"] != 0 else np.nan

        results[element] = stats

    return pd.DataFrame(results)


def group_statistics(
    df: pd.DataFrame,
    group_by: str,
    elements: Optional[List[str]] = None,
    agg_functions: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Calculate statistics grouped by a categorical column.

    Args:
        df: Input DataFrame
        group_by: Column name to group by (e.g., "sample_type", "collector")
        elements: Element columns to analyze (default: DEFAULT_ELEMENTS)
        agg_functions: Aggregation functions to apply
                      (default: ["count", "mean", "std", "min", "max"])

    Returns:
        pd.DataFrame: Multi-level DataFrame with groups as rows,
                     elements and statistics as columns

    Example:
        >>> grouped = group_statistics(df, group_by="sample_type")
        >>> print(grouped.loc["soil"])  # Stats for soil samples
    """
    # TODO: Implement grouped statistics
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. If agg_functions is None, use ["count", "mean", "std", "min", "max"]
    # 3. Filter to only include elements that exist in DataFrame
    # 4. Use df.groupby(group_by)[elements].agg(agg_functions)
    # 5. Return the result
    #
    # Hints:
    # - Use list comprehension to filter elements: [e for e in elements if e in df.columns]
    # - The result will have MultiIndex columns (element, statistic)

    if elements is None:
        elements = DEFAULT_ELEMENTS

    if agg_functions is None:
        agg_functions = ["count", "mean", "std", "min", "max"]

    elements = [e for e in elements if e in df.columns]

    grouped = df.groupby(group_by)[elements].agg(agg_functions)

    return grouped


def correlation_analysis(
    df: pd.DataFrame,
    elements: Optional[List[str]] = None,
    method: str = "pearson"
) -> pd.DataFrame:
    """
    Compute correlation matrix between element columns.

    Args:
        df: Input DataFrame
        elements: Element columns to include (default: DEFAULT_ELEMENTS)
        method: Correlation method - "pearson", "spearman", or "kendall"

    Returns:
        pd.DataFrame: Correlation matrix

    Example:
        >>> corr_matrix = correlation_analysis(df, method="spearman")
        >>> print(corr_matrix.loc["Au_ppb", "Cu_ppm"])  # Au-Cu correlation
    """
    # TODO: Implement correlation analysis
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. Filter to elements that exist in DataFrame
    # 3. Use df[elements].corr(method=method)
    # 4. Return correlation matrix
    #
    # Hints:
    # - pandas corr() method supports "pearson", "spearman", "kendall"

    if elements is None:
        elements = DEFAULT_ELEMENTS

    elements = [e for e in elements if e in df.columns]

    if len(elements) < 2:
        return pd.DataFrame()

    return df[elements].corr(method=method)



def identify_significant_correlations(
    df: pd.DataFrame,
    elements: Optional[List[str]] = None,
    threshold: float = 0.5,
    p_value_threshold: float = 0.05
) -> List[Dict[str, Any]]:
    """
    Identify statistically significant correlations between elements.

    Args:
        df: Input DataFrame
        elements: Element columns to analyze (default: DEFAULT_ELEMENTS)
        threshold: Minimum absolute correlation to report (default: 0.5)
        p_value_threshold: Maximum p-value for significance (default: 0.05)

    Returns:
        List of dictionaries containing:
            - element_1: First element name
            - element_2: Second element name
            - correlation: Correlation coefficient
            - p_value: Statistical p-value
            - significant: Whether p-value < threshold

    Example:
        >>> significant = identify_significant_correlations(df, threshold=0.6)
        >>> for corr in significant:
        ...     print(f"{corr['element_1']} - {corr['element_2']}: r={corr['correlation']:.3f}")
    """
    # TODO: Implement significant correlation identification
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. Filter to elements that exist in DataFrame
    # 3. Initialize empty results list
    # 4. For each unique pair of elements (avoid duplicates):
    #    a. Use scipy_stats.pearsonr() to get correlation and p-value
    #    b. If abs(correlation) >= threshold:
    #       - Add to results list with element names, r, p-value, significance
    # 5. Sort results by absolute correlation (descending)
    # 6. Return results list
    #
    # Hints:
    # - Use itertools.combinations(elements, 2) for unique pairs
    # - scipy_stats.pearsonr() returns (correlation, p_value)
    # - Handle NaN values by dropping them before correlation calculation

    if elements is None:
        elements = DEFAULT_ELEMENTS

    elements = [e for e in elements if e in df.columns]

    results = []

    for a, b in itertools.combinations(elements, 2):

        pair = df[[a, b]].dropna()

        if len(pair) < 3:
            continue

        try:
            r, p = scipy_stats.pearsonr(pair[a], pair[b])
        except Exception:
            continue

        if np.isnan(r) or np.isnan(p):
            continue

        if abs(r) >= threshold:
            results.append({
                "element_1": a,
                "element_2": b,
                "correlation": r,
                "p_value": p,
                "significant": p < p_value_threshold
            })

    results.sort(key=lambda x: abs(x["correlation"]), reverse=True)

    return results




def analyze_by_spatial_region(
    df: pd.DataFrame,
    n_regions: int = 4,
    elements: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Divide samples into spatial regions and analyze each region.

    Divides the study area into a grid based on UTM coordinates
    and calculates statistics for each region.

    Args:
        df: Input DataFrame with utm_e and utm_n columns
        n_regions: Number of regions per axis (creates n_regions^2 grid cells)
        elements: Elements to analyze (default: DEFAULT_ELEMENTS)

    Returns:
        Dictionary with region IDs as keys and statistics DataFrames as values

    Example:
        >>> regional_stats = analyze_by_spatial_region(df, n_regions=3)
        >>> print(regional_stats["region_0_1"])  # Stats for region at row 0, col 1
    """
    # TODO: Implement spatial region analysis
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. Calculate UTM coordinate ranges
    # 3. Create bins for easting and northing
    # 4. Assign each sample to a region based on its coordinates
    # 5. For each region with data:
    #    a. Calculate statistics using calculate_statistics()
    #    b. Store in results dictionary
    # 6. Return dictionary of regional statistics
    #
    # Hints:
    # - Use pd.cut() to bin coordinates
    # - Region ID could be f"region_{row}_{col}"

    if elements is None:
        elements = DEFAULT_ELEMENTS

    elements = [e for e in elements if e in df.columns]

    if "utm_e" not in df.columns or "utm_n" not in df.columns:
        raise ValueError("DataFrame must contain 'utm_e' and 'utm_n' columns")

    e_min, e_max = df["utm_e"].min(), df["utm_e"].max()
    n_min, n_max = df["utm_n"].min(), df["utm_n"].max()

    e_bins = np.linspace(e_min, e_max, n_regions + 1)
    n_bins = np.linspace(n_min, n_max, n_regions + 1)

    df = df.copy()

    df["region_e"] = pd.cut(df["utm_e"], bins=e_bins, labels=False, include_lowest=True)
    df["region_n"] = pd.cut(df["utm_n"], bins=n_bins, labels=False, include_lowest=True)

    results = {}

    for (i, j), group in df.groupby(["region_n", "region_e"]):

        if group.empty:
            continue

        region_id = f"region_{i}_{j}"

        stats_df = calculate_statistics(group, elements=elements)

        results[region_id] = stats_df

    return results

def generate_analysis_summary(
    df: pd.DataFrame,
    elements: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Generate a comprehensive analysis summary.

    Combines all analysis functions into a single summary report.

    Args:
        df: Input DataFrame
        elements: Elements to analyze (default: DEFAULT_ELEMENTS)

    Returns:
        Dictionary containing:
            - descriptive_stats: Full statistics DataFrame
            - correlations: Correlation matrix
            - significant_correlations: List of significant correlations
            - sample_type_stats: Statistics by sample type
            - collector_stats: Statistics by collector

    Example:
        >>> summary = generate_analysis_summary(df)
        >>> print(summary["descriptive_stats"])
    """
    # TODO: Implement analysis summary generation
    #
    # Steps:
    # 1. Call calculate_statistics()
    # 2. Call correlation_analysis()
    # 3. Call identify_significant_correlations()
    # 4. Call group_statistics() for sample_type
    # 5. Call group_statistics() for collector
    # 6. Compile all results into summary dictionary
    # 7. Return summary
    summary = {}
    summary["descriptive_stats"] = calculate_statistics(df, elements=elements)
    summary["correlations"] = correlation_analysis(df, elements=elements)
    summary["significant_correlations"] = identify_significant_correlations(df, elements=elements)
    if "sample_type" in df.columns:
        summary["sample_type_stats"] = group_statistics(df, "sample_type", elements=elements)
    else:
        summary["sample_type_stats"] = pd.DataFrame()
    if "collector" in df.columns:
        summary["collector_stats"] = group_statistics(df, "collector", elements=elements)
    else:
        summary["collector_stats"] = pd.DataFrame()

    return summary
