"""
Visualization Module
====================

This module creates publication-quality visualizations for
geochemical data analysis, including histograms, spatial plots,
and multi-panel summary figures.

Functions:
    create_summary_plot: Create multi-panel summary visualization
    plot_element_histograms: Plot distribution histograms for elements
    plot_spatial_distribution: Plot sample locations with color-coded values
    plot_anomaly_map: Plot detected anomalies on spatial map
    apply_professional_style: Apply consistent styling to figures

Example:
    >>> from pipeline.visualizer import create_summary_plot, plot_spatial_distribution
    >>> fig = create_summary_plot(df, stats, anomalies)
    >>> fig.savefig("output/analysis_plot.png", dpi=300)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from typing import List, Optional, Dict, Any, Tuple


# Default element columns for visualization
DEFAULT_ELEMENTS = ["Au_ppb", "Cu_ppm", "Pb_ppm", "Zn_ppm", "As_ppm"]

# Professional color palette
COLORS = {
    "primary": "#2C3E50",
    "secondary": "#3498DB",
    "accent": "#E74C3C",
    "success": "#27AE60",
    "warning": "#F39C12",
    "neutral": "#95A5A6",
}

# Element-specific colors
ELEMENT_COLORS = {
    "Au_ppb": "#FFD700",  # Gold
    "Cu_ppm": "#B87333",  # Copper
    "Pb_ppm": "#4A4A4A",  # Lead (gray)
    "Zn_ppm": "#7EB4D2",  # Zinc (light blue)
    "As_ppm": "#8B4513",  # Arsenic (brown)
    "Fe_pct": "#C0392B",  # Iron (rust red)
}


def apply_professional_style(ax: Axes, title: str = "", xlabel: str = "", ylabel: str = "") -> None:
    """
    Apply consistent professional styling to an axis.

    Args:
        ax: Matplotlib Axes object
        title: Plot title
        xlabel: X-axis label
        ylabel: Y-axis label

    Example:
        >>> fig, ax = plt.subplots()
        >>> ax.plot(x, y)
        >>> apply_professional_style(ax, "My Plot", "X", "Y")
    """
    # TODO: Implement professional styling
    #
    # Steps:
    # 1. Set title with appropriate font size and weight
    # 2. Set axis labels with appropriate font size
    # 3. Remove top and right spines
    # 4. Set tick parameters (direction, size)
    # 5. Add light grid on y-axis only
    #
    # Hints:
    # - ax.set_title(title, fontsize=12, fontweight='bold')
    # - ax.spines['top'].set_visible(False)
    # - ax.tick_params(direction='out', length=4)
    # - ax.yaxis.grid(True, linestyle='--', alpha=0.7)

    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_xlabel(xlabel, fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.tick_params(direction="out", length=4, width=1)

    ax.yaxis.grid(True, linestyle="--", alpha=0.5)


def plot_element_histograms(
    df: pd.DataFrame,
    elements: Optional[List[str]] = None,
    bins: int = 30,
    figsize: Tuple[int, int] = (12, 8)
) -> Figure:
    """
    Create histograms showing distribution of element concentrations.

    Args:
        df: Input DataFrame with element columns
        elements: Elements to plot (default: DEFAULT_ELEMENTS)
        bins: Number of histogram bins (default: 30)
        figsize: Figure size in inches (default: (12, 8))

    Returns:
        matplotlib Figure object

    Example:
        >>> fig = plot_element_histograms(df, elements=["Au_ppb", "Cu_ppm"])
        >>> fig.savefig("histograms.png")
    """
    # TODO: Implement histogram plotting
    #
    # Steps:
    # 1. If elements is None, use DEFAULT_ELEMENTS
    # 2. Filter to elements present in DataFrame
    # 3. Create subplot grid (2 columns, enough rows for all elements)
    # 4. For each element:
    #    a. Create histogram with appropriate color
    #    b. Add mean line (dashed)
    #    c. Add median line (solid)
    #    d. Apply professional styling
    #    e. Add legend showing mean/median values
    # 5. Adjust layout and return figure
    #
    # Hints:
    # - Use plt.subplots(nrows, 2, figsize=figsize)
    # - Use ax.hist(data, bins=bins, color=color, alpha=0.7, edgecolor='black')
    # - Use ax.axvline(mean, color='red', linestyle='--', label=f'Mean: {mean:.2f}')

    if elements is None:
        elements = DEFAULT_ELEMENTS

    elements = [e for e in elements if e in df.columns]

    n = len(elements)
    cols = 2
    rows = (n + 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=figsize)
    axes = np.array(axes).flatten()

    for i, element in enumerate(elements):

        data = df[element].dropna()

        ax = axes[i]

        ax.hist(
            data,
            bins=bins,
            color=ELEMENT_COLORS.get(element, "blue"),
            alpha=0.7,
            edgecolor="black"
        )

        mean = data.mean()
        median = data.median()

        ax.axvline(mean, color="red", linestyle="--", label=f"Mean: {mean:.2f}")
        ax.axvline(median, color="black", linestyle="-", label=f"Median: {median:.2f}")

        apply_professional_style(ax, title=element, xlabel="Value", ylabel="Frequency")
        ax.legend()

    # hide unused axes
    for j in range(i + 1, len(axes)):
        axes[j].axis("off")

    fig.tight_layout()
    return fig


def plot_spatial_distribution(
    df: pd.DataFrame,
    element: str,
    figsize: Tuple[int, int] = (10, 8),
    cmap: str = "viridis",
    anomaly_threshold: Optional[float] = None
) -> Figure:
    """
    Create spatial plot showing element distribution across sample locations.

    Args:
        df: Input DataFrame with utm_e, utm_n, and element columns
        element: Element column to visualize
        figsize: Figure size in inches
        cmap: Colormap name (default: "viridis")
        anomaly_threshold: If provided, mark samples above this threshold

    Returns:
        matplotlib Figure object

    Example:
        >>> fig = plot_spatial_distribution(df, "Au_ppb", anomaly_threshold=100)
        >>> fig.savefig("au_spatial.png")
    """
    # TODO: Implement spatial distribution plot
    #
    # Steps:
    # 1. Create figure and axis
    # 2. Create scatter plot with:
    #    - x = utm_e, y = utm_n
    #    - color = element values
    #    - size = fixed or scaled by value
    # 3. Add colorbar with element name and units
    # 4. If anomaly_threshold provided:
    #    - Mark anomalies with different marker (e.g., red triangle)
    #    - Add to legend
    # 5. Apply professional styling
    # 6. Set aspect ratio to 'equal' for proper spatial representation
    # 7. Return figure
    #
    # Hints:
    # - Use ax.scatter(x, y, c=values, cmap=cmap, s=50, alpha=0.7)
    # - Use plt.colorbar(scatter, ax=ax, label=element)
    # - ax.set_aspect('equal')

    fig, ax = plt.subplots(figsize=figsize)

    sc = ax.scatter(
        df["utm_e"],
        df["utm_n"],
        c=df[element],
        cmap=cmap,
        s=50,
        alpha=0.8
    )

    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label(element)

    if anomaly_threshold is not None:
        anomalies = df[df[element] > anomaly_threshold]

        ax.scatter(
            anomalies["utm_east"],
            anomalies["utm_north"],
            c="red",
            marker="^",
            s=100,
            label="Anomalies"
        )

        ax.legend()

    apply_professional_style(ax, f"Spatial Distribution: {element}", "UTM Easting", "UTM Northing")

    ax.set_aspect("equal")

    return fig


def plot_anomaly_map(
    df: pd.DataFrame,
    anomalies: pd.DataFrame,
    element: str,
    figsize: Tuple[int, int] = (10, 8)
) -> Figure:
    """
    Create map highlighting anomalous samples.

    Args:
        df: Full DataFrame with all samples
        anomalies: DataFrame of detected anomalies
        element: Element to display
        figsize: Figure size in inches

    Returns:
        matplotlib Figure object

    Example:
        >>> fig = plot_anomaly_map(df, anomalies, "Au_ppb")
        >>> fig.savefig("anomaly_map.png")
    """
    # TODO: Implement anomaly map
    #
    # Steps:
    # 1. Create figure and axis
    # 2. Plot all samples as background (gray, small)
    # 3. Filter anomalies for the specified element
    # 4. Plot anomalies with:
    #    - Color scaled by value
    #    - Larger marker size
    #    - Different marker shape (e.g., diamond)
    # 5. Add colorbar for anomaly values
    # 6. Add legend distinguishing normal vs anomalous
    # 7. Apply professional styling
    # 8. Return figure
    #
    # Hints:
    # - Plot background: ax.scatter(..., c='lightgray', s=20, alpha=0.5, label='Normal')
    # - Plot anomalies: ax.scatter(..., c=values, cmap='Reds', s=80, marker='D')

    fig, ax = plt.subplots(figsize=figsize)

    ax.scatter(
        df["utm_east"],
        df["utm_north"],
        c="lightgray",
        s=20,
        alpha=0.5,
        label="Normal"
    )

    filtered = anomalies[anomalies["element"] == element]

    if not filtered.empty:
        sc = ax.scatter(
            filtered["utm_east"],
            filtered["utm_north"],
            c=filtered["value"],
            cmap="Reds",
            s=80,
            marker="D",
            label="Anomalies"
        )

        plt.colorbar(sc, ax=ax, label=element)

    ax.legend()

    apply_professional_style(ax, f"Anomaly Map: {element}", "UTM Easting", "UTM Northing")

    ax.set_aspect("equal")

    return fig


def plot_correlation_heatmap(
    correlation_matrix: pd.DataFrame,
    figsize: Tuple[int, int] = (8, 6)
) -> Figure:
    """
    Create heatmap visualization of correlation matrix.

    Args:
        correlation_matrix: Correlation DataFrame from analyzer module
        figsize: Figure size in inches

    Returns:
        matplotlib Figure object

    Example:
        >>> corr = correlation_analysis(df)
        >>> fig = plot_correlation_heatmap(corr)
        >>> fig.savefig("correlations.png")
    """
    # TODO: Implement correlation heatmap
    #
    # Steps:
    # 1. Create figure and axis
    # 2. Use imshow() to create heatmap
    # 3. Set colormap to diverging (e.g., 'RdBu_r')
    # 4. Set color scale from -1 to 1
    # 5. Add colorbar
    # 6. Add tick labels for elements
    # 7. Annotate cells with correlation values
    # 8. Apply professional styling
    # 9. Return figure
    #
    # Hints:
    # - im = ax.imshow(correlation_matrix, cmap='RdBu_r', vmin=-1, vmax=1)
    # - ax.set_xticks(range(len(columns))), ax.set_xticklabels(columns, rotation=45)
    # - Use ax.text() to annotate each cell

    fig, ax = plt.subplots(figsize=figsize)

    im = ax.imshow(correlation_matrix, cmap="RdBu_r", vmin=-1, vmax=1)

    plt.colorbar(im, ax=ax)

    labels = correlation_matrix.columns

    ax.set_xticks(range(len(labels)))
    ax.set_yticks(range(len(labels)))

    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_yticklabels(labels)

    for i in range(len(labels)):
        for j in range(len(labels)):
            ax.text(j, i, f"{correlation_matrix.iloc[i, j]:.2f}",
                    ha="center", va="center", fontsize=8)

    apply_professional_style(ax, "Correlation Heatmap", "", "")

    return fig


def create_summary_plot(
    df: pd.DataFrame,
    statistics: pd.DataFrame,
    anomalies: pd.DataFrame,
    elements: Optional[List[str]] = None,
    figsize: Tuple[int, int] = (16, 12)
) -> Figure:
    """
    Create comprehensive multi-panel summary visualization.

    Creates a figure with:
    - Histograms for each element
    - Spatial distribution of primary element
    - Anomaly locations
    - Summary statistics table

    Args:
        df: Cleaned DataFrame
        statistics: Statistics DataFrame from analyzer
        anomalies: Anomaly DataFrame from detector
        elements: Elements to include (default: first 3 of DEFAULT_ELEMENTS)
        figsize: Figure size in inches

    Returns:
        matplotlib Figure object

    Example:
        >>> fig = create_summary_plot(df, stats, anomalies)
        >>> fig.savefig("output/analysis_plot.png", dpi=300, bbox_inches='tight')
    """
    # TODO: Implement comprehensive summary plot
    #
    # Steps:
    # 1. If elements is None, use first 3 of DEFAULT_ELEMENTS
    # 2. Create figure with GridSpec for flexible layout:
    #    - Top row: 3 histograms
    #    - Middle row: Spatial plot (large) + Anomaly map
    #    - Bottom row: Statistics table
    # 3. Create histogram subplots for each element
    # 4. Create spatial distribution plot for primary element
    # 5. Create anomaly map
    # 6. Create statistics table visualization
    # 7. Add overall title
    # 8. Adjust layout
    # 9. Return figure
    #
    # Hints:
    # - from matplotlib.gridspec import GridSpec
    # - gs = GridSpec(3, 3, figure=fig)
    # - ax1 = fig.add_subplot(gs[0, 0])  # First histogram
    # - ax_spatial = fig.add_subplot(gs[1, :2])  # Spatial plot (spans 2 columns)

    from matplotlib.gridspec import GridSpec

    if elements is None:
        elements = DEFAULT_ELEMENTS[:3]

    fig = plt.figure(figsize=figsize)
    gs = GridSpec(3, 3, figure=fig)

    # Histograms
    for i, element in enumerate(elements):
        ax = fig.add_subplot(gs[0, i])

        ax.hist(df[element].dropna(), bins=30,
                color=ELEMENT_COLORS.get(element, "blue"),
                alpha=0.7)

        apply_professional_style(ax, element, "", "")

    # Spatial plot
    ax1 = fig.add_subplot(gs[1, :2])
    sc = ax1.scatter(df["utm_east"], df["utm_north"],
                     c=df[elements[0]], cmap="viridis")

    plt.colorbar(sc, ax=ax1)
    apply_professional_style(ax1, "Spatial Distribution", "E", "N")
    ax1.set_aspect("equal")

    # Anomaly map
    ax2 = fig.add_subplot(gs[1, 2])

    ax2.scatter(df["utm_e"], df["utm_n"], c="lightgray", s=20)

    if not anomalies.empty:
        ax2.scatter(anomalies["utm_e"], anomalies["utm_n"],
                    c="red", s=50, marker="D")

    apply_professional_style(ax2, "Anomalies", "E", "N")

    # Stats table
    ax3 = fig.add_subplot(gs[2, :])
    ax3.axis("off")

    table_data = statistics.head(5)

    ax3.table(cellText=table_data.values,
              colLabels=table_data.columns,
              loc="center")

    fig.suptitle("Geochemical Analysis Summary", fontsize=14, fontweight="bold")

    fig.tight_layout()

    return fig


def save_figure(
    fig: Figure,
    filepath: str,
    dpi: int = 300,
    transparent: bool = False
) -> None:
    """
    Save figure with publication-quality settings.

    Args:
        fig: Figure to save
        filepath: Output file path
        dpi: Resolution in dots per inch (default: 300)
        transparent: Whether background should be transparent

    Example:
        >>> fig = create_summary_plot(df, stats, anomalies)
        >>> save_figure(fig, "output/analysis_plot.png")
    """
    # TODO: Implement figure saving
    #
    # Steps:
    # 1. Ensure output directory exists
    # 2. Save figure with:
    #    - Specified DPI
    #    - bbox_inches='tight' to avoid cutoff
    #    - facecolor handling for transparency
    # 3. Close figure to free memory
    #
    # Hints:
    # - from pathlib import Path
    # - Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    # - fig.savefig(filepath, dpi=dpi, bbox_inches='tight', transparent=transparent)
    # - plt.close(fig)

    from pathlib import Path

    Path(filepath).parent.mkdir(parents=True, exist_ok=True)

    fig.savefig(
        filepath,
        dpi=dpi,
        bbox_inches="tight",
        transparent=transparent
    )

    plt.close(fig)
