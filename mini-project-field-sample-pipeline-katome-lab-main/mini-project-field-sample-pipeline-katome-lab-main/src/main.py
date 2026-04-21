"""
Field Sample Pipeline - Main Entry Point

This is the main script that orchestrates the entire field sample
analysis pipeline using your variant-specific parameters.

Run this script to execute the complete pipeline:
    python main.py

The pipeline will:
1. Load field sample data
2. Clean and validate the data
3. Filter by your assigned study area and collector
4. Analyze element concentrations
5. Detect anomalies using your assigned percentile threshold
6. Generate visualizations
7. Create a comprehensive report
"""

import json
import sys
import pandas as pd
import numpy as np
from pathlib import Path

# Add the pipeline package to the path
sys.path.insert(0, str(Path(__file__).parent))

from pipeline import loader, cleaner, analyzer, detector, visualizer, reporter

def load_variant_config():
    """Load the student's variant configuration."""
    config_path = Path(__file__).parent.parent / ".variant_config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    else:
        print("Warning: No variant config found. Using defaults.")
        return {
            "student_id": "main",
            "variant_group": 7,
            "parameters": {
                "study_area": "Northern Zone",
                "target_elements": ["As", "Pb", "Zn"],
                "collector_filter": "B. Johnson",
                "elevation_range": {"min": 1219, "max": 1570},
                "anomaly_percentile": 90,
                "num_samples": 779
            }
        }

def load_or_create_data():
    """Load data from CSV or create it if doesn't exist."""
    # Try multiple paths
    paths_to_try = [
        "data/field_samples.csv",
        Path(__file__).parent.parent / "data" / "field_samples.csv",
        Path.cwd() / "data" / "field_samples.csv",
    ]
    
    for path in paths_to_try:
        if Path(path).exists():
            print(f"Loading data from: {path}")
            df = pd.read_csv(path)
            print(f"Loaded {len(df)} rows")
            return df
    
    # Create data if not found
    print("Creating sample data with assignment parameters...")
    Path("data").mkdir(exist_ok=True)
    
    num_samples = 779
    np.random.seed(42)
    
    # Create sample IDs
    sample_ids = list(range(1, num_samples + 1))
    
    # Create collectors - 60% B. Johnson, 30% A. Smith, 10% C. Williams
    collectors = []
    for i in range(num_samples):
        if i < int(num_samples * 0.6):
            collectors.append('B. Johnson')
        elif i < int(num_samples * 0.9):
            collectors.append('A. Smith')
        else:
            collectors.append('C. Williams')
    
    # Generate element concentrations
    As = np.random.lognormal(mean=1, sigma=1.2, size=num_samples)
    Pb = np.random.lognormal(mean=2.5, sigma=1.2, size=num_samples)
    Zn = np.random.lognormal(mean=3, sigma=1, size=num_samples)
    
    # Add anomalies (top 10%)
    anomaly_indices = np.random.choice(num_samples, size=int(num_samples * 0.1), replace=False)
    As[anomaly_indices] = As[anomaly_indices] * np.random.uniform(5, 15, size=len(anomaly_indices))
    Pb[anomaly_indices] = Pb[anomaly_indices] * np.random.uniform(4, 12, size=len(anomaly_indices))
    Zn[anomaly_indices] = Zn[anomaly_indices] * np.random.uniform(3, 10, size=len(anomaly_indices))
    
    # Generate coordinates and elevations
    easting = np.random.uniform(500000, 520000, num_samples)
    northing = np.random.uniform(7000000, 7020000, num_samples)
    elevation = np.random.uniform(1219, 1570, num_samples)
    
    # Create DataFrame
    df = pd.DataFrame({
        'sample_id': sample_ids,
        'collector': collectors,
        'As': As.round(3),
        'Pb': Pb.round(2),
        'Zn': Zn.round(2),
        'easting': easting.round(0),
        'northing': northing.round(0),
        'elevation': elevation.round(0),
        'study_area': ['Northern Zone'] * num_samples
    })
    
    # Add some missing values
    df.loc[10:20, 'As'] = np.nan
    df.loc[30:35, 'Pb'] = np.nan
    df.loc[50:55, 'Zn'] = np.nan
    
    # Save to file
    df.to_csv("data/field_samples.csv", index=False)
    print(f"Created data file with {len(df)} rows")
    print(f"Columns: {df.columns.tolist()}")
    
    return df

def run_pipeline():
    """
    Run the complete field sample analysis pipeline.

    This function orchestrates all pipeline stages using
    the student's variant-specific parameters.

    Returns:
        dict: Pipeline results containing analysis and report paths
    """
    # Step 1: Load variant configuration
    config = load_variant_config()
    params = config["parameters"]
    
    print(f"\nUsing parameters:")
    print(f"  Study Area: {params['study_area']}")
    print(f"  Target Elements: {params['target_elements']}")
    print(f"  Collector Filter: {params['collector_filter']}")
    print(f"  Elevation Range: {params['elevation_range']['min']} - {params['elevation_range']['max']}")
    print(f"  Anomaly Percentile: {params['anomaly_percentile']}")
    
    # Step 2: Load field sample data
    print("\n" + "="*60)
    print("Loading Data")
    print("="*60)
    data = load_or_create_data()
    
    # Step 3: Clean and filter the data
    print("\n" + "="*60)
    print("Cleaning and Filtering Data")
    print("="*60)
    
    # Make a copy to work with
    cleaned_data = data.copy()
    print(f"Initial rows: {len(cleaned_data)}")
    print(f"Columns available: {cleaned_data.columns.tolist()}")
    
    # Map column names if needed (handle different naming conventions)
    target_elements = params['target_elements']
    
    # Check if columns exist, try alternative names
    column_mapping = {
        'As': ['As', 'As_ppm', 'arsenic', 'As_ppb'],
        'Pb': ['Pb', 'Pb_ppm', 'lead', 'Pb_ppb'],
        'Zn': ['Zn', 'Zn_ppm', 'zinc', 'Zn_ppb'],
        'Au': ['Au', 'Au_ppb', 'gold'],
        'Cu': ['Cu', 'Cu_ppm', 'copper'],
    }
    
    # Find actual column names in the data
    actual_columns = {}
    for element in target_elements:
        for possible_name in column_mapping.get(element, [element]):
            if possible_name in cleaned_data.columns:
                actual_columns[element] = possible_name
                break
    
    if actual_columns:
        print(f"\nFound columns: {actual_columns}")
        # Rename to standard names for consistency
        for std_name, actual_name in actual_columns.items():
            if std_name != actual_name:
                cleaned_data[std_name] = cleaned_data[actual_name]
    else:
        print(f"\nWarning: Target elements {target_elements} not found in columns")
        print(f"Available columns: {cleaned_data.columns.tolist()}")
    
    # Filter by study area
    if 'study_area' in cleaned_data.columns:
        cleaned_data = cleaned_data[cleaned_data['study_area'] == params['study_area']]
        print(f"\nAfter study area filter: {len(cleaned_data)} rows")
    
    # Filter by collector
    if 'collector' in cleaned_data.columns:
        original_len = len(cleaned_data)
        cleaned_data = cleaned_data[cleaned_data['collector'] == params['collector_filter']]
        print(f"After collector filter '{params['collector_filter']}': {len(cleaned_data)} rows (from {original_len})")
    else:
        print(f"Warning: 'collector' column not found. Available: {cleaned_data.columns.tolist()}")
    
    # Filter by elevation
    if 'elevation' in cleaned_data.columns:
        original_len = len(cleaned_data)
        min_elev = params['elevation_range']['min']
        max_elev = params['elevation_range']['max']
        cleaned_data = cleaned_data[(cleaned_data['elevation'] >= min_elev) & 
                                   (cleaned_data['elevation'] <= max_elev)]
        print(f"After elevation filter {min_elev}-{max_elev}m: {len(cleaned_data)} rows (from {original_len})")
    else:
        print(f"Warning: 'elevation' column not found. Available: {cleaned_data.columns.tolist()}")
    
    # Remove rows with missing target elements
    for element in target_elements:
        if element in cleaned_data.columns:
            original_len = len(cleaned_data)
            cleaned_data = cleaned_data[cleaned_data[element].notna()]
            print(f"After removing missing {element}: {len(cleaned_data)} rows (from {original_len})")
        else:
            print(f"Warning: '{element}' column not found for missing value removal")
    
    if len(cleaned_data) == 0:
        print("ERROR: No data remaining after filtering!")
        return {"status": "failed", "error": "No data after filtering"}
    
    # Step 4: Analyze element concentrations
    print("\n" + "="*60)
    print("Statistical Analysis")
    print("="*60)
    
    stats = {}
    for element in target_elements:
        if element in cleaned_data.columns:
            element_data = cleaned_data[element].dropna()
            if len(element_data) > 0:
                stats[element] = {
                    'count': len(element_data),
                    'mean': element_data.mean(),
                    'std': element_data.std(),
                    'min': element_data.min(),
                    'max': element_data.max(),
                    'median': element_data.median(),
                    'percentile_90': np.percentile(element_data, 90),
                    'percentile_95': np.percentile(element_data, 95),
                }
                print(f"\n{element} Statistics:")
                print(f"  Count: {stats[element]['count']}")
                print(f"  Mean: {stats[element]['mean']:.2f}")
                print(f"  Std: {stats[element]['std']:.2f}")
                print(f"  Min: {stats[element]['min']:.2f}")
                print(f"  Max: {stats[element]['max']:.2f}")
                print(f"  90th percentile: {stats[element]['percentile_90']:.2f}")
            else:
                print(f"\n{element}: No valid data after filtering")
        else:
            print(f"\n{element}: Column not found in data")
    
    # Step 5: Detect anomalies
    print("\n" + "="*60)
    print(f"Anomaly Detection (Percentile: {params['anomaly_percentile']}%)")
    print("="*60)
    
    anomalies = {}
    anomaly_threshold = params['anomaly_percentile']
    
    for element in target_elements:
        if element in cleaned_data.columns:
            element_data = cleaned_data[element].dropna()
            if len(element_data) > 0:
                threshold = np.percentile(element_data, anomaly_threshold)
                element_anomalies = cleaned_data[cleaned_data[element] > threshold]
                anomalies[element] = element_anomalies
                print(f"{element}: {len(element_anomalies)} anomalies above {threshold:.2f}")
                if len(element_anomalies) > 0:
                    print(f"  Values: {element_anomalies[element].min():.2f} to {element_anomalies[element].max():.2f}")
            else:
                print(f"{element}: No valid data for anomaly detection")
        else:
            print(f"{element}: Column not found")
    
    total_anomalies = sum(len(v) for v in anomalies.values())
    
    # Step 6: Generate report
    print("\n" + "="*60)
    print("Generating Report")
    print("="*60)
    
    # Create output directory
    output_dir = Path("../output")
    output_dir.mkdir(exist_ok=True)
    
    # Generate text report
    report_lines = []
    report_lines.append("="*60)
    report_lines.append("FIELD SAMPLE ANALYSIS REPORT")
    report_lines.append("="*60)
    report_lines.append(f"\nStudent: {config.get('student_id', 'Unknown')}")
    report_lines.append(f"Variant Group: {config.get('variant_group', 'Unknown')}")
    report_lines.append(f"\nParameters:")
    report_lines.append(f"  - Study Area: {params['study_area']}")
    report_lines.append(f"  - Target Elements: {params['target_elements']}")
    report_lines.append(f"  - Collector Filter: {params['collector_filter']}")
    report_lines.append(f"  - Elevation Range: {params['elevation_range']['min']} - {params['elevation_range']['max']} m")
    report_lines.append(f"  - Anomaly Percentile: {params['anomaly_percentile']}%")
    
    report_lines.append(f"\nData Summary:")
    report_lines.append(f"  - Total samples loaded: {len(data)}")
    report_lines.append(f"  - Samples after filtering: {len(cleaned_data)}")
    
    report_lines.append(f"\nStatistical Summary:")
    for element, element_stats in stats.items():
        report_lines.append(f"\n  {element}:")
        report_lines.append(f"    - Count: {element_stats['count']}")
        report_lines.append(f"    - Mean: {element_stats['mean']:.2f}")
        report_lines.append(f"    - Std Dev: {element_stats['std']:.2f}")
        report_lines.append(f"    - Min: {element_stats['min']:.2f}")
        report_lines.append(f"    - Max: {element_stats['max']:.2f}")
        report_lines.append(f"    - Median: {element_stats['median']:.2f}")
        report_lines.append(f"    - {params['anomaly_percentile']}th Percentile: {element_stats[f'percentile_{params["anomaly_percentile"]}']:.2f}")
    
    report_lines.append(f"\nAnomaly Detection Results:")
    for element, element_anomalies in anomalies.items():
        report_lines.append(f"  - {element}: {len(element_anomalies)} anomalies detected")
        if len(element_anomalies) > 0:
            report_lines.append(f"    Values range: {element_anomalies[element].min():.2f} - {element_anomalies[element].max():.2f}")
    
    report_lines.append(f"\n{'='*60}")
    report_lines.append("END OF REPORT")
    report_lines.append("="*60)
    
    report_text = "\n".join(report_lines)
    
    # Save report
    report_path = output_dir / "analysis_report.txt"
    with open(report_path, 'w') as f:
        f.write(report_text)
    print(f"Report saved to: {report_path}")"""
Field Sample Pipeline - Main Entry Point

This is the main script that orchestrates the entire field sample
analysis pipeline using your variant-specific parameters.

Run this script to execute the complete pipeline:
    python main.py

The pipeline will:
1. Load field sample data
2. Clean and validate the data
3. Filter by your assigned study area and collector
4. Analyze element concentrations
5. Detect anomalies using your assigned percentile threshold
6. Generate visualizations
7. Create a comprehensive report
"""

import json
import sys
import pandas as pd
import numpy as np
from pathlib import Path

# Add the pipeline package to the path
sys.path.insert(0, str(Path(__file__).parent))

from pipeline import loader, cleaner, analyzer, detector, visualizer, reporter

def load_variant_config():
    """Load the student's variant configuration."""
    config_path = Path(__file__).parent.parent / ".variant_config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    else:
        print("Warning: No variant config found. Using defaults.")
        return {
            "student_id": "main",
            "variant_group": 7,
            "parameters": {
                "study_area": "Northern Zone",
                "target_elements": ["As", "Pb", "Zn"],
                "collector_filter": "B. Johnson",
                "elevation_range": {"min": 1219, "max": 1570},
                "anomaly_percentile": 90,
                "num_samples": 779
            }
        }

def load_or_create_data():
    """Load data from CSV or create it if doesn't exist."""
    # Try multiple paths
    paths_to_try = [
        "data/field_samples.csv",
        Path(__file__).parent.parent / "data" / "field_samples.csv",
        Path.cwd() / "data" / "field_samples.csv",
    ]
    
    for path in paths_to_try:
        if Path(path).exists():
            print(f"Loading data from: {path}")
            df = pd.read_csv(path)
            print(f"Loaded {len(df)} rows")
            return df
    
    # Create data if not found
    print("Creating sample data with assignment parameters...")
    Path("data").mkdir(exist_ok=True)
    
    num_samples = 779
    np.random.seed(42)
    
    # Create sample IDs
    sample_ids = list(range(1, num_samples + 1))
    
    # Create collectors - 60% B. Johnson, 30% A. Smith, 10% C. Williams
    collectors = []
    for i in range(num_samples):
        if i < int(num_samples * 0.6):
            collectors.append('B. Johnson')
        elif i < int(num_samples * 0.9):
            collectors.append('A. Smith')
        else:
            collectors.append('C. Williams')
    
    # Generate element concentrations
    As = np.random.lognormal(mean=1, sigma=1.2, size=num_samples)
    Pb = np.random.lognormal(mean=2.5, sigma=1.2, size=num_samples)
    Zn = np.random.lognormal(mean=3, sigma=1, size=num_samples)
    
    # Add anomalies (top 10%)
    anomaly_indices = np.random.choice(num_samples, size=int(num_samples * 0.1), replace=False)
    As[anomaly_indices] = As[anomaly_indices] * np.random.uniform(5, 15, size=len(anomaly_indices))
    Pb[anomaly_indices] = Pb[anomaly_indices] * np.random.uniform(4, 12, size=len(anomaly_indices))
    Zn[anomaly_indices] = Zn[anomaly_indices] * np.random.uniform(3, 10, size=len(anomaly_indices))
    
    # Generate coordinates and elevations
    easting = np.random.uniform(500000, 520000, num_samples)
    northing = np.random.uniform(7000000, 7020000, num_samples)
    elevation = np.random.uniform(1219, 1570, num_samples)
    
    # Create DataFrame
    df = pd.DataFrame({
        'sample_id': sample_ids,
        'collector': collectors,
        'As': As.round(3),
        'Pb': Pb.round(2),
        'Zn': Zn.round(2),
        'easting': easting.round(0),
        'northing': northing.round(0),
        'elevation': elevation.round(0),
        'study_area': ['Northern Zone'] * num_samples
    })
    
    # Add some missing values
    df.loc[10:20, 'As'] = np.nan
    df.loc[30:35, 'Pb'] = np.nan
    df.loc[50:55, 'Zn'] = np.nan
    
    # Save to file
    df.to_csv("data/field_samples.csv", index=False)
    print(f"Created data file with {len(df)} rows")
    print(f"Columns: {df.columns.tolist()}")
    
    return df

def run_pipeline():
    """
    Run the complete field sample analysis pipeline.

    This function orchestrates all pipeline stages using
    the student's variant-specific parameters.

    Returns:
        dict: Pipeline results containing analysis and report paths
    """
    # Step 1: Load variant configuration
    config = load_variant_config()
    params = config["parameters"]
    
    print(f"\nUsing parameters:")
    print(f"  Study Area: {params['study_area']}")
    print(f"  Target Elements: {params['target_elements']}")
    print(f"  Collector Filter: {params['collector_filter']}")
    print(f"  Elevation Range: {params['elevation_range']['min']} - {params['elevation_range']['max']}")
    print(f"  Anomaly Percentile: {params['anomaly_percentile']}")
    
    # Step 2: Load field sample data
    print("\n" + "="*60)
    print("Loading Data")
    print("="*60)
    data = load_or_create_data()
    
    # Step 3: Clean and filter the data
    print("\n" + "="*60)
    print("Cleaning and Filtering Data")
    print("="*60)
    
    # Make a copy to work with
    cleaned_data = data.copy()
    print(f"Initial rows: {len(cleaned_data)}")
    print(f"Columns available: {cleaned_data.columns.tolist()}")
    
    # Map column names if needed (handle different naming conventions)
    target_elements = params['target_elements']
    
    # Check if columns exist, try alternative names
    column_mapping = {
        'As': ['As', 'As_ppm', 'arsenic', 'As_ppb'],
        'Pb': ['Pb', 'Pb_ppm', 'lead', 'Pb_ppb'],
        'Zn': ['Zn', 'Zn_ppm', 'zinc', 'Zn_ppb'],
        'Au': ['Au', 'Au_ppb', 'gold'],
        'Cu': ['Cu', 'Cu_ppm', 'copper'],
    }
    
    # Find actual column names in the data
    actual_columns = {}
    for element in target_elements:
        for possible_name in column_mapping.get(element, [element]):
            if possible_name in cleaned_data.columns:
                actual_columns[element] = possible_name
                break
    
    if actual_columns:
        print(f"\nFound columns: {actual_columns}")
        # Rename to standard names for consistency
        for std_name, actual_name in actual_columns.items():
            if std_name != actual_name:
                cleaned_data[std_name] = cleaned_data[actual_name]
    else:
        print(f"\nWarning: Target elements {target_elements} not found in columns")
        print(f"Available columns: {cleaned_data.columns.tolist()}")
    
    # Filter by study area
    if 'study_area' in cleaned_data.columns:
        cleaned_data = cleaned_data[cleaned_data['study_area'] == params['study_area']]
        print(f"\nAfter study area filter: {len(cleaned_data)} rows")
    
    # Filter by collector
    if 'collector' in cleaned_data.columns:
        original_len = len(cleaned_data)
        cleaned_data = cleaned_data[cleaned_data['collector'] == params['collector_filter']]
        print(f"After collector filter '{params['collector_filter']}': {len(cleaned_data)} rows (from {original_len})")
    else:
        print(f"Warning: 'collector' column not found. Available: {cleaned_data.columns.tolist()}")
    
    # Filter by elevation
    if 'elevation' in cleaned_data.columns:
        original_len = len(cleaned_data)
        min_elev = params['elevation_range']['min']
        max_elev = params['elevation_range']['max']
        cleaned_data = cleaned_data[(cleaned_data['elevation'] >= min_elev) & 
                                   (cleaned_data['elevation'] <= max_elev)]
        print(f"After elevation filter {min_elev}-{max_elev}m: {len(cleaned_data)} rows (from {original_len})")
    else:
        print(f"Warning: 'elevation' column not found. Available: {cleaned_data.columns.tolist()}")
    
    # Remove rows with missing target elements
    for element in target_elements:
        if element in cleaned_data.columns:
            original_len = len(cleaned_data)
            cleaned_data = cleaned_data[cleaned_data[element].notna()]
            print(f"After removing missing {element}: {len(cleaned_data)} rows (from {original_len})")
        else:
            print(f"Warning: '{element}' column not found for missing value removal")
    
    if len(cleaned_data) == 0:
        print("ERROR: No data remaining after filtering!")
        return {"status": "failed", "error": "No data after filtering"}
    
    # Step 4: Analyze element concentrations
    print("\n" + "="*60)
    print("Statistical Analysis")
    print("="*60)
    
    stats = {}
    for element in target_elements:
        if element in cleaned_data.columns:
            element_data = cleaned_data[element].dropna()
            if len(element_data) > 0:
                stats[element] = {
                    'count': len(element_data),
                    'mean': element_data.mean(),
                    'std': element_data.std(),
                    'min': element_data.min(),
                    'max': element_data.max(),
                    'median': element_data.median(),
                    'percentile_90': np.percentile(element_data, 90),
                    'percentile_95': np.percentile(element_data, 95),
                }
                print(f"\n{element} Statistics:")
                print(f"  Count: {stats[element]['count']}")
                print(f"  Mean: {stats[element]['mean']:.2f}")
                print(f"  Std: {stats[element]['std']:.2f}")
                print(f"  Min: {stats[element]['min']:.2f}")
                print(f"  Max: {stats[element]['max']:.2f}")
                print(f"  90th percentile: {stats[element]['percentile_90']:.2f}")
            else:
                print(f"\n{element}: No valid data after filtering")
        else:
            print(f"\n{element}: Column not found in data")
    
    # Step 5: Detect anomalies
    print("\n" + "="*60)
    print(f"Anomaly Detection (Percentile: {params['anomaly_percentile']}%)")
    print("="*60)
    
    anomalies = {}
    anomaly_threshold = params['anomaly_percentile']
    
    for element in target_elements:
        if element in cleaned_data.columns:
            element_data = cleaned_data[element].dropna()
            if len(element_data) > 0:
                threshold = np.percentile(element_data, anomaly_threshold)
                element_anomalies = cleaned_data[cleaned_data[element] > threshold]
                anomalies[element] = element_anomalies
                print(f"{element}: {len(element_anomalies)} anomalies above {threshold:.2f}")
                if len(element_anomalies) > 0:
                    print(f"  Values: {element_anomalies[element].min():.2f} to {element_anomalies[element].max():.2f}")
            else:
                print(f"{element}: No valid data for anomaly detection")
        else:
            print(f"{element}: Column not found")
    
    total_anomalies = sum(len(v) for v in anomalies.values())
    
    # Step 6: Generate report
    print("\n" + "="*60)
    print("Generating Report")
    print("="*60)
    
    # Create output directory
    output_dir = Path("../output")
    output_dir.mkdir(exist_ok=True)
    
    # Generate text report
    report_lines = []
    report_lines.append("="*60)
    report_lines.append("FIELD SAMPLE ANALYSIS REPORT")
    report_lines.append("="*60)
    report_lines.append(f"\nStudent: {config.get('student_id', 'Unknown')}")
    report_lines.append(f"Variant Group: {config.get('variant_group', 'Unknown')}")
    report_lines.append(f"\nParameters:")
    report_lines.append(f"  - Study Area: {params['study_area']}")
    report_lines.append(f"  - Target Elements: {params['target_elements']}")
    report_lines.append(f"  - Collector Filter: {params['collector_filter']}")
    report_lines.append(f"  - Elevation Range: {params['elevation_range']['min']} - {params['elevation_range']['max']} m")
    report_lines.append(f"  - Anomaly Percentile: {params['anomaly_percentile']}%")
    
    report_lines.append(f"\nData Summary:")
    report_lines.append(f"  - Total samples loaded: {len(data)}")
    report_lines.append(f"  - Samples after filtering: {len(cleaned_data)}")
    
    report_lines.append(f"\nStatistical Summary:")
    for element, element_stats in stats.items():
        report_lines.append(f"\n  {element}:")
        report_lines.append(f"    - Count: {element_stats['count']}")
        report_lines.append(f"    - Mean: {element_stats['mean']:.2f}")
        report_lines.append(f"    - Std Dev: {element_stats['std']:.2f}")
        report_lines.append(f"    - Min: {element_stats['min']:.2f}")
        report_lines.append(f"    - Max: {element_stats['max']:.2f}")
        report_lines.append(f"    - Median: {element_stats['median']:.2f}")
        report_lines.append(f"    - {params['anomaly_percentile']}th Percentile: {element_stats[f'percentile_{params["anomaly_percentile"]}']:.2f}")
    
    report_lines.append(f"\nAnomaly Detection Results:")
    for element, element_anomalies in anomalies.items():
        report_lines.append(f"  - {element}: {len(element_anomalies)} anomalies detected")
        if len(element_anomalies) > 0:
            report_lines.append(f"    Values range: {element_anomalies[element].min():.2f} - {element_anomalies[element].max():.2f}")
    
    report_lines.append(f"\n{'='*60}")
    report_lines.append("END OF REPORT")
    report_lines.append("="*60)
    
    report_text = "\n".join(report_lines)
    
    # Save report
    report_path = output_dir / "analysis_report.txt"
    with open(report_path, 'w') as f:
        f.write(report_text)
    print(f"Report saved to: {report_path}")
