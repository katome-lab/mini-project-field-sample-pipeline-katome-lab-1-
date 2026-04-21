"""
GGY3061 Field Sample Analysis Pipeline
======================================

A professional-grade data analysis pipeline for processing
field geochemical samples from raw data to publication-quality outputs.

Modules:
    loader: Data loading and validation
    cleaner: Data cleaning and preprocessing
    analyzer: Statistical analysis
    detector: Anomaly detection
    visualizer: Visualization generation
    reporter: Report generation

Example:
    >>> from pipeline import loader, cleaner, analyzer, detector
    >>> data = loader.load_samples("data/field_samples.csv")
    >>> cleaned = cleaner.clean_data(data)
    >>> stats = analyzer.calculate_statistics(cleaned)
    >>> anomalies = detector.detect_anomalies(cleaned)
"""

from pipeline.loader import load_samples, validate_data
from pipeline.cleaner import clean_data, filter_by_criteria
from pipeline.analyzer import calculate_statistics, correlation_analysis
from pipeline.detector import detect_anomalies, AnomalyDetector
from pipeline.visualizer import create_summary_plot, plot_spatial_distribution
from pipeline.reporter import (
    generate_text_report,
    generate_html_report,
    save_report,
    create_executive_summary
)


__version__ = "1.0.0"
__author__ = "KATOME PAUL"

__all__ = [
    "load_samples",
    "validate_data",
    "clean_data",
    "filter_by_criteria",
    "calculate_statistics",
    "correlation_analysis",
    "detect_anomalies",
    "AnomalyDetector",
    "create_summary_plot",
    "plot_spatial_distribution",
    "generate_text_report",
    "generate_html_report",
    "save_report",
    "create_executive_summary",
]
