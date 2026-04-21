"""
Mini-Project Visible Tests - Field Sample Pipeline
"""

import subprocess
import sys
from pathlib import Path
import pytest
import pandas as pd
import numpy as np

SRC_DIR = Path(__file__).parent.parent.parent / "src"
DATA_DIR = Path(__file__).parent.parent.parent / "data"

# Conditionally import modules - they may not be implemented yet
sys.path.insert(0, str(SRC_DIR))

# Try importing loader functions
try:
    from pipeline.loader import load_samples, validate_data, get_loading_statistics
    HAS_LOADER = True
except ImportError:
    HAS_LOADER = False

# Try importing cleaner functions
try:
    from pipeline.cleaner import (
        handle_missing_values, detect_outliers, filter_by_criteria,
        clean_data, standardize_data_types, handle_outliers
    )
    HAS_CLEANER = True
except ImportError:
    HAS_CLEANER = False

# Try importing analyzer functions
try:
    from pipeline.analyzer import (
        calculate_statistics, group_statistics, correlation_analysis,
        identify_significant_correlations
    )
    HAS_ANALYZER = True
except ImportError:
    HAS_ANALYZER = False

# Try importing detector functions
try:
    from pipeline.detector import (
        detect_anomalies, calculate_thresholds, AnomalyDetector,
        get_anomaly_spatial_context
    )
    HAS_DETECTOR = True
except ImportError:
    HAS_DETECTOR = False

# Try importing reporter functions
try:
    from pipeline.reporter import (
        generate_text_report, save_report
    )
    HAS_REPORTER = True
except ImportError:
    HAS_REPORTER = False


# ============================================================================
# STRUCTURAL TESTS (keep existing tests)
# ============================================================================

class TestPipelineStructure:
    """Tests for pipeline module structure."""

    def test_pipeline_package_exists(self):
        """pipeline package should exist."""
        assert (SRC_DIR / "pipeline").exists()

    def test_loader_module_exists(self):
        """loader.py should exist."""
        assert (SRC_DIR / "pipeline" / "loader.py").exists()

    def test_cleaner_module_exists(self):
        """cleaner.py should exist."""
        assert (SRC_DIR / "pipeline" / "cleaner.py").exists()

    def test_analyzer_module_exists(self):
        """analyzer.py should exist."""
        assert (SRC_DIR / "pipeline" / "analyzer.py").exists()

    def test_detector_module_exists(self):
        """detector.py should exist."""
        assert (SRC_DIR / "pipeline" / "detector.py").exists()

    def test_visualizer_module_exists(self):
        """visualizer.py should exist."""
        assert (SRC_DIR / "pipeline" / "visualizer.py").exists()

    def test_reporter_module_exists(self):
        """reporter.py should exist."""
        assert (SRC_DIR / "pipeline" / "reporter.py").exists()


class TestMainScript:
    """Tests for main pipeline script."""

    def test_main_exists(self):
        """main.py should exist."""
        assert (SRC_DIR / "main.py").exists()


class TestDataFiles:
    """Tests for data files."""

    def test_field_samples_exists(self):
        """field_samples.csv should exist."""
        assert (DATA_DIR / "field_samples.csv").exists()


# ============================================================================
# FIXTURES - Create shared test data
# ============================================================================

@pytest.fixture
def sample_csv(tmp_path):
    """Create a small test CSV with field sample data."""
    np.random.seed(42)
    n = 15
    data = {
        'sample_id': [f'S{i:03d}' for i in range(1, n+1)],
        'utm_e': np.random.uniform(500000, 510000, n),
        'utm_n': np.random.uniform(8200000, 8210000, n),
        'elevation': np.random.uniform(1000, 1500, n),
        'Au_ppb': np.random.lognormal(0, 1, n).round(3),
        'Cu_ppm': np.random.lognormal(4, 0.8, n).round(1),
        'Pb_ppm': np.random.lognormal(3, 0.5, n).round(1),
        'Zn_ppm': np.random.lognormal(4, 0.7, n).round(1),
        'As_ppm': np.random.lognormal(2, 1, n).round(1),
        'Fe_pct': np.random.uniform(1, 10, n).round(2),
        'collector': np.random.choice(['A. Smith', 'B. Johnson', 'C. Williams'], n),
        'collection_date': pd.date_range('2024-01-01', periods=n, freq='D'),
        'sample_type': np.random.choice(['soil', 'rock', 'stream'], n),
    }
    df = pd.DataFrame(data)
    csv_path = tmp_path / "test_samples.csv"
    df.to_csv(csv_path, index=False)
    return str(csv_path), df


@pytest.fixture
def sample_dataframe():
    """Create test DataFrame directly."""
    np.random.seed(42)
    n = 15
    data = {
        'sample_id': [f'S{i:03d}' for i in range(1, n+1)],
        'utm_e': np.random.uniform(500000, 510000, n),
        'utm_n': np.random.uniform(8200000, 8210000, n),
        'elevation': np.random.uniform(1000, 1500, n),
        'Au_ppb': np.random.lognormal(0, 1, n).round(3),
        'Cu_ppm': np.random.lognormal(4, 0.8, n).round(1),
        'Pb_ppm': np.random.lognormal(3, 0.5, n).round(1),
        'Zn_ppm': np.random.lognormal(4, 0.7, n).round(1),
        'As_ppm': np.random.lognormal(2, 1, n).round(1),
        'Fe_pct': np.random.uniform(1, 10, n).round(2),
        'collector': np.random.choice(['A. Smith', 'B. Johnson', 'C. Williams'], n),
        'collection_date': pd.date_range('2024-01-01', periods=n, freq='D'),
        'sample_type': np.random.choice(['soil', 'rock', 'stream'], n),
    }
    return pd.DataFrame(data)


# ============================================================================
# LOADER FUNCTION TESTS
# ============================================================================

class TestLoaderFunctions:
    """Tests for data loading and validation functions."""

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_load_samples_returns_dataframe(self, sample_csv):
        """load_samples should return a pandas DataFrame."""
        csv_path, _ = sample_csv
        result = load_samples(csv_path)
        assert isinstance(result, pd.DataFrame), "load_samples must return a DataFrame"

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_load_samples_correct_rows(self, sample_csv):
        """load_samples should load all rows from CSV."""
        csv_path, expected_df = sample_csv
        result = load_samples(csv_path)
        assert len(result) == len(expected_df), "Incorrect number of rows loaded"

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_load_samples_correct_columns(self, sample_csv):
        """load_samples should load all columns from CSV."""
        csv_path, expected_df = sample_csv
        result = load_samples(csv_path)
        assert list(result.columns) == list(expected_df.columns), "Columns mismatch"

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_load_samples_file_not_found(self):
        """load_samples should raise FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            load_samples("/nonexistent/path/to/file.csv")

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_validate_data_valid_dataframe(self, sample_dataframe):
        """validate_data should return (True, []) for valid DataFrame."""
        is_valid, errors = validate_data(sample_dataframe)
        assert isinstance(is_valid, bool), "First return value must be bool"
        assert isinstance(errors, list), "Second return value must be list"
        assert is_valid is True, "Should validate correct DataFrame"
        assert len(errors) == 0, "Should have no error messages"

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_validate_data_missing_required_column(self, sample_dataframe):
        """validate_data should fail when required column is missing."""
        df = sample_dataframe.drop('sample_id', axis=1)
        is_valid, errors = validate_data(df)
        assert is_valid is False, "Should fail with missing column"
        assert len(errors) > 0, "Should have error messages"

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_validate_data_missing_element_column(self, sample_dataframe):
        """validate_data should warn about missing element columns."""
        df = sample_dataframe.drop('Au_ppb', axis=1)
        is_valid, errors = validate_data(df)
        assert is_valid is False, "Should fail with missing element column"

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_get_loading_statistics_returns_dict(self, sample_dataframe):
        """get_loading_statistics should return a dictionary."""
        result = get_loading_statistics(sample_dataframe)
        assert isinstance(result, dict), "Should return a dictionary"

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_get_loading_statistics_contains_expected_keys(self, sample_dataframe):
        """get_loading_statistics should contain key statistics."""
        result = get_loading_statistics(sample_dataframe)
        expected_keys = ['total_samples', 'unique_collectors', 'sample_types', 'missing_values']
        for key in expected_keys:
            assert key in result, f"Missing key '{key}' in statistics"

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_get_loading_statistics_correct_sample_count(self, sample_dataframe):
        """get_loading_statistics should report correct sample count."""
        result = get_loading_statistics(sample_dataframe)
        assert result['total_samples'] == len(sample_dataframe)

    @pytest.mark.skipif(not HAS_LOADER, reason="Loader module not importable")
    def test_get_loading_statistics_unique_collectors(self, sample_dataframe):
        """get_loading_statistics should count unique collectors."""
        result = get_loading_statistics(sample_dataframe)
        expected = sample_dataframe['collector'].nunique()
        assert result['unique_collectors'] == expected


# ============================================================================
# CLEANER FUNCTION TESTS
# ============================================================================

class TestCleanerFunctions:
    """Tests for data cleaning and preprocessing functions."""

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_handle_missing_values_returns_tuple(self, sample_dataframe):
        """handle_missing_values should return (DataFrame, dict)."""
        result = handle_missing_values(sample_dataframe, strategy="median")
        assert isinstance(result, tuple), "Should return tuple"
        assert len(result) == 2, "Should return 2-element tuple"
        assert isinstance(result[0], pd.DataFrame), "First element should be DataFrame"
        assert isinstance(result[1], dict), "Second element should be dict"

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_handle_missing_values_preserves_shape(self, sample_dataframe):
        """handle_missing_values should preserve DataFrame shape for no NaN case."""
        result_df, _ = handle_missing_values(sample_dataframe, strategy="median")
        assert result_df.shape[0] == sample_dataframe.shape[0] or \
               result_df.shape[0] < sample_dataframe.shape[0], \
               "Row count should not increase"

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_handle_missing_values_invalid_strategy(self, sample_dataframe):
        """handle_missing_values should raise ValueError for invalid strategy."""
        with pytest.raises(ValueError):
            handle_missing_values(sample_dataframe, strategy="invalid_strategy")

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_detect_outliers_returns_boolean_dataframe(self, sample_dataframe):
        """detect_outliers should return boolean DataFrame."""
        result = detect_outliers(sample_dataframe, method="iqr")
        assert isinstance(result, pd.DataFrame), "Should return DataFrame"
        assert result.shape[0] == sample_dataframe.shape[0], "Should have same row count"
        assert result.dtypes.unique()[0] == bool, "Should contain boolean values"

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_detect_outliers_iqr_method(self, sample_dataframe):
        """detect_outliers should work with IQR method."""
        result = detect_outliers(sample_dataframe, method="iqr", threshold=1.5)
        assert result is not None, "Should return valid result"

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_detect_outliers_zscore_method(self, sample_dataframe):
        """detect_outliers should work with zscore method."""
        result = detect_outliers(sample_dataframe, method="zscore", threshold=3.0)
        assert result is not None, "Should return valid result"

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_filter_by_criteria_returns_dataframe(self, sample_dataframe):
        """filter_by_criteria should return a DataFrame."""
        result = filter_by_criteria(sample_dataframe)
        assert isinstance(result, pd.DataFrame), "Should return DataFrame"

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_filter_by_criteria_no_filters(self, sample_dataframe):
        """filter_by_criteria with no filters should return all rows."""
        result = filter_by_criteria(sample_dataframe)
        assert len(result) == len(sample_dataframe), "Should return all rows when no filter"

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_filter_by_criteria_by_collector(self, sample_dataframe):
        """filter_by_criteria should filter by collector."""
        collector = sample_dataframe['collector'].iloc[0]
        result = filter_by_criteria(sample_dataframe, collector=collector)
        assert all(result['collector'] == collector), "Should filter by collector"
        assert len(result) <= len(sample_dataframe), "Filtered result should be smaller"

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_filter_by_criteria_by_elevation_range(self, sample_dataframe):
        """filter_by_criteria should filter by elevation range."""
        result = filter_by_criteria(
            sample_dataframe,
            elevation_range=(1100, 1400)
        )
        assert all(result['elevation'] >= 1100), "Should respect min elevation"
        assert all(result['elevation'] <= 1400), "Should respect max elevation"

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_clean_data_returns_tuple(self, sample_dataframe):
        """clean_data should return (DataFrame, dict)."""
        result = clean_data(sample_dataframe)
        assert isinstance(result, tuple), "Should return tuple"
        assert len(result) == 2, "Should return 2-element tuple"
        assert isinstance(result[0], pd.DataFrame), "First element should be DataFrame"
        assert isinstance(result[1], dict), "Second element should be dict"

    @pytest.mark.skipif(not HAS_CLEANER, reason="Cleaner module not importable")
    def test_clean_data_report_contains_keys(self, sample_dataframe):
        """clean_data report should contain key statistics."""
        _, report = clean_data(sample_dataframe)
        expected_keys = ['original_rows', 'final_rows']
        for key in expected_keys:
            assert key in report, f"Missing key '{key}' in cleaning report"


# ============================================================================
# ANALYZER FUNCTION TESTS
# ============================================================================

class TestAnalyzerFunctions:
    """Tests for statistical analysis functions."""

    @pytest.mark.skipif(not HAS_ANALYZER, reason="Analyzer module not importable")
    def test_calculate_statistics_returns_dataframe(self, sample_dataframe):
        """calculate_statistics should return a DataFrame."""
        result = calculate_statistics(sample_dataframe)
        assert isinstance(result, pd.DataFrame), "Should return DataFrame"

    @pytest.mark.skipif(not HAS_ANALYZER, reason="Analyzer module not importable")
    def test_calculate_statistics_contains_descriptive_stats(self, sample_dataframe):
        """calculate_statistics should contain basic statistical metrics."""
        result = calculate_statistics(sample_dataframe)
        expected_stats = ['count', 'mean', 'std', 'min', 'max']
        for stat in expected_stats:
            assert stat in result.index, f"Missing statistic '{stat}'"

    @pytest.mark.skipif(not HAS_ANALYZER, reason="Analyzer module not importable")
    def test_calculate_statistics_has_elements(self, sample_dataframe):
        """calculate_statistics should have element columns."""
        result = calculate_statistics(sample_dataframe)
        expected_elements = ['Au_ppb', 'Cu_ppm', 'Pb_ppm', 'Zn_ppm', 'As_ppm', 'Fe_pct']
        for elem in expected_elements:
            assert elem in result.columns, f"Missing element '{elem}' in statistics"

    @pytest.mark.skipif(not HAS_ANALYZER, reason="Analyzer module not importable")
    def test_group_statistics_returns_dataframe(self, sample_dataframe):
        """group_statistics should return a DataFrame."""
        result = group_statistics(sample_dataframe, group_by='sample_type')
        assert isinstance(result, pd.DataFrame), "Should return DataFrame"

    @pytest.mark.skipif(not HAS_ANALYZER, reason="Analyzer module not importable")
    def test_group_statistics_has_groups(self, sample_dataframe):
        """group_statistics should have group rows."""
        result = group_statistics(sample_dataframe, group_by='sample_type')
        expected_groups = sample_dataframe['sample_type'].unique()
        for group in expected_groups:
            assert group in result.index.get_level_values(0) or group in result.index, \
                   f"Missing group '{group}' in results"

    @pytest.mark.skipif(not HAS_ANALYZER, reason="Analyzer module not importable")
    def test_correlation_analysis_returns_dataframe(self, sample_dataframe):
        """correlation_analysis should return a DataFrame."""
        result = correlation_analysis(sample_dataframe)
        assert isinstance(result, pd.DataFrame), "Should return DataFrame"

    @pytest.mark.skipif(not HAS_ANALYZER, reason="Analyzer module not importable")
    def test_correlation_analysis_is_square_matrix(self, sample_dataframe):
        """correlation_analysis should return square matrix."""
        result = correlation_analysis(sample_dataframe)
        assert result.shape[0] == result.shape[1], "Should return square matrix"

    @pytest.mark.skipif(not HAS_ANALYZER, reason="Analyzer module not importable")
    def test_correlation_analysis_diagonal_is_one(self, sample_dataframe):
        """correlation_analysis diagonal should be 1 (self-correlation)."""
        result = correlation_analysis(sample_dataframe)
        np.testing.assert_array_almost_equal(
            np.diag(result),
            np.ones(result.shape[0]),
            decimal=5,
            err_msg="Diagonal should be all 1s"
        )

    @pytest.mark.skipif(not HAS_ANALYZER, reason="Analyzer module not importable")
    def test_identify_significant_correlations_returns_list(self, sample_dataframe):
        """identify_significant_correlations should return list."""
        result = identify_significant_correlations(sample_dataframe)
        assert isinstance(result, list), "Should return list"

    @pytest.mark.skipif(not HAS_ANALYZER, reason="Analyzer module not importable")
    def test_identify_significant_correlations_dict_entries(self, sample_dataframe):
        """identify_significant_correlations should contain dict entries."""
        result = identify_significant_correlations(sample_dataframe, threshold=0.1)
        if len(result) > 0:
            expected_keys = ['element_1', 'element_2', 'correlation']
            for entry in result:
                assert isinstance(entry, dict), "Entries should be dicts"
                for key in expected_keys:
                    assert key in entry, f"Missing key '{key}' in correlation entry"


# ============================================================================
# DETECTOR FUNCTION TESTS
# ============================================================================

class TestDetectorFunctions:
    """Tests for anomaly detection functions."""

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_detect_anomalies_returns_dataframe(self, sample_dataframe):
        """detect_anomalies should return a DataFrame."""
        result = detect_anomalies(sample_dataframe, percentile=95)
        assert isinstance(result, pd.DataFrame), "Should return DataFrame"

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_detect_anomalies_has_required_columns(self, sample_dataframe):
        """detect_anomalies result should have required columns."""
        result = detect_anomalies(sample_dataframe, percentile=95)
        expected_cols = ['sample_id', 'element', 'value', 'threshold']
        for col in expected_cols:
            assert col in result.columns or col in result.index.names, \
                   f"Missing column '{col}' in anomalies"

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_detect_anomalies_finite_values(self, sample_dataframe):
        """detect_anomalies should return finite anomaly values."""
        result = detect_anomalies(sample_dataframe, percentile=95)
        if len(result) > 0:
            numeric_cols = result.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                assert np.all(np.isfinite(result[col])), \
                       f"Non-finite values in column '{col}'"

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_calculate_thresholds_returns_dict(self, sample_dataframe):
        """calculate_thresholds should return a dictionary."""
        result = calculate_thresholds(sample_dataframe, percentile=95)
        assert isinstance(result, dict), "Should return dictionary"

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_calculate_thresholds_has_elements(self, sample_dataframe):
        """calculate_thresholds should have element keys."""
        result = calculate_thresholds(sample_dataframe, percentile=95)
        expected_elements = ['Au_ppb', 'Cu_ppm', 'Pb_ppm', 'Zn_ppm', 'As_ppm']
        for elem in expected_elements:
            if elem in sample_dataframe.columns:
                assert elem in result, f"Missing threshold for '{elem}'"

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_anomaly_detector_creation(self):
        """AnomalyDetector should be instantiable."""
        detector = AnomalyDetector(percentile=95)
        assert detector is not None
        assert detector.percentile == 95

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_anomaly_detector_fit(self, sample_dataframe):
        """AnomalyDetector.fit should work."""
        detector = AnomalyDetector(percentile=95)
        result = detector.fit(sample_dataframe)
        assert result is detector, "fit() should return self"

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_anomaly_detector_detect_after_fit(self, sample_dataframe):
        """AnomalyDetector.detect should work after fit."""
        detector = AnomalyDetector(percentile=95)
        detector.fit(sample_dataframe)
        result = detector.detect(sample_dataframe)
        assert isinstance(result, pd.DataFrame), "detect() should return DataFrame"

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_anomaly_detector_detect_before_fit_raises(self, sample_dataframe):
        """AnomalyDetector.detect should raise ValueError if not fitted."""
        detector = AnomalyDetector(percentile=95)
        with pytest.raises(ValueError):
            detector.detect(sample_dataframe)

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_anomaly_detector_fit_detect(self, sample_dataframe):
        """AnomalyDetector.fit_detect should work."""
        detector = AnomalyDetector(percentile=95)
        result = detector.fit_detect(sample_dataframe)
        assert isinstance(result, pd.DataFrame), "fit_detect() should return DataFrame"

    @pytest.mark.skipif(not HAS_DETECTOR, reason="Detector module not importable")
    def test_anomaly_detector_get_thresholds_after_fit(self, sample_dataframe):
        """AnomalyDetector.get_thresholds should work after fit."""
        detector = AnomalyDetector(percentile=95)
        detector.fit(sample_dataframe)
        thresholds = detector.get_thresholds()
        assert isinstance(thresholds, dict), "Should return dict"
        assert len(thresholds) > 0, "Should have thresholds"


# ============================================================================
# REPORTER FUNCTION TESTS
# ============================================================================

class TestReporterFunctions:
    """Tests for report generation functions."""

    @pytest.mark.skipif(not HAS_REPORTER, reason="Reporter module not importable")
    def test_generate_text_report_accepts_params(self, sample_dataframe):
        """generate_text_report should accept required parameters."""
        analysis_results = {'mean': 5.0, 'std': 1.5}
        anomalies = pd.DataFrame({'sample_id': ['S001'], 'element': ['Au_ppb']})
        variant_config = {'elements': ['Au_ppb']}

        # Should not raise an error
        result = generate_text_report(analysis_results, anomalies, variant_config)
        # Result should be string or None, but not raise
        assert result is None or isinstance(result, str), \
               "generate_text_report should return str or None"

    @pytest.mark.skipif(not HAS_REPORTER, reason="Reporter module not importable")
    def test_save_report_accepts_params(self, tmp_path):
        """save_report should accept required parameters."""
        report_content = "Test Report"
        output_path = tmp_path / "test_report.txt"

        # Should not raise an error
        result = save_report(str(report_content), str(output_path))
        # Result should be bool or None, but not raise
        assert result is None or isinstance(result, bool), \
               "save_report should return bool or None"


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestIntegration:
    """Integration tests across multiple modules."""

    @pytest.mark.skipif(not (HAS_LOADER and HAS_CLEANER and HAS_ANALYZER),
                        reason="Not all modules importable")
    def test_pipeline_workflow_load_clean_analyze(self, sample_csv):
        """Test basic pipeline workflow: load -> clean -> analyze."""
        csv_path, _ = sample_csv

        # Load
        df = load_samples(csv_path)
        assert df is not None and len(df) > 0

        # Clean
        cleaned_df, _ = clean_data(df)
        assert cleaned_df is not None

        # Analyze
        stats = calculate_statistics(cleaned_df)
        assert stats is not None

    @pytest.mark.skipif(not (HAS_LOADER and HAS_DETECTOR),
                        reason="Not all modules importable")
    def test_pipeline_workflow_with_anomaly_detection(self, sample_csv):
        """Test pipeline with anomaly detection."""
        csv_path, _ = sample_csv

        # Load
        df = load_samples(csv_path)
        assert df is not None

        # Detect anomalies
        anomalies = detect_anomalies(df, percentile=90)
        assert anomalies is not None
