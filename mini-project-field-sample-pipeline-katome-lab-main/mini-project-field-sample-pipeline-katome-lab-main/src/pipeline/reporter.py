"""
Reporter Module - Generate reports from analysis results.

This module handles report generation for the field sample pipeline.

Students should implement:
- generate_text_report(): Create a text summary report
- generate_html_report(): Create an HTML report with formatting
- save_report(): Save report to file
- create_executive_summary(): Create a brief executive summary
"""


def generate_text_report(analysis_results, anomalies, variant_config):
    """
    Generate a text report from analysis results.

    Parameters:
        analysis_results: Dictionary containing analysis statistics
        anomalies: DataFrame of detected anomalies
        variant_config: Student's variant configuration

    Returns:
        str: Formatted text report
    """
    # TODO: Implement text report generation
    # Include: study area, target elements, sample counts, statistics, anomaly summary
    lines = []

    lines.append("=" * 60)
    lines.append("FIELD SAMPLE ANALYSIS REPORT")
    lines.append("=" * 60)

    # Variant info
    lines.append(f"Study Area: {variant_config.get('study_area', 'N/A')}")
    lines.append(f"Target Elements: {variant_config.get('elements', 'N/A')}")
    lines.append("")

    # Sample stats
    if analysis_results:
        lines.append("SUMMARY STATISTICS")
        lines.append("-" * 30)

        if "descriptive_stats" in analysis_results:
            stats = analysis_results["descriptive_stats"]
            lines.append(f"Elements analyzed: {len(stats.columns)}")

    # Anomalies
    lines.append("")
    lines.append("ANOMALY SUMMARY")
    lines.append("-" * 30)

    if anomalies is not None and not anomalies.empty:
        lines.append(f"Total anomalies: {len(anomalies)}")
        lines.append(f"Unique elements affected: {anomalies['element'].nunique()}")
    else:
        lines.append("No anomalies detected.")

    lines.append("=" * 60)

    return "\n".join(lines)


def generate_html_report(analysis_results, anomalies, figures, variant_config):
    """
    Generate an HTML report with embedded figures.

    Parameters:
        analysis_results: Dictionary containing analysis statistics
        anomalies: DataFrame of detected anomalies
        figures: List of matplotlib figure objects or paths
        variant_config: Student's variant configuration

    Returns:
        str: HTML formatted report
    """
    # TODO: Implement HTML report generation
    html = []

    html.append("<html>")
    html.append("<head><title>Field Sample Report</title></head>")
    html.append("<body>")

    html.append("<h1>Field Sample Analysis Report</h1>")

    html.append("<h2>Study Information</h2>")
    html.append(f"<p><b>Area:</b> {variant_config.get('study_area', 'N/A')}</p>")
    html.append(f"<p><b>Elements:</b> {variant_config.get('elements', 'N/A')}</p>")

    html.append("<h2>Anomalies</h2>")

    if anomalies is not None and not anomalies.empty:
        html.append(f"<p>Total anomalies: {len(anomalies)}</p>")

        html.append("<table border='1'>")
        html.append("<tr><th>Element</th><th>Value</th><th>Threshold</th></tr>")

        for _, row in anomalies.head(10).iterrows():
            html.append(
                f"<tr><td>{row['element']}</td>"
                f"<td>{row['value']}</td>"
                f"<td>{row['threshold']}</td></tr>"
            )

        html.append("</table>")
    else:
        html.append("<p>No anomalies detected.</p>")

    html.append("<h2>Figures</h2>")

    if figures:
        for fig in figures:
            html.append(f"<p>{str(fig)}</p>")

    html.append("</body></html>")

    return "\n".join(html)


def save_report(report_content, output_path, format_type="text"):
    """
    Save report to a file.

    Parameters:
        report_content: String content of the report
        output_path: Path to save the report
        format_type: "text" or "html"

    Returns:
        bool: True if save successful, False otherwise
    """
    # TODO: Implement report saving
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report_content)
        return True

    except Exception as e:
        print(f"Error saving report: {e}")
        return False


def create_executive_summary(analysis_results, anomalies, variant_config):
    """
    Create a brief executive summary of findings.

    Parameters:
        analysis_results: Dictionary containing analysis statistics
        anomalies: DataFrame of detected anomalies
        variant_config: Student's variant configuration

    Returns:
        str: Brief executive summary (3-5 sentences)
    """
    # TODO: Implement executive summary
    total_anomalies = len(anomalies) if anomalies is not None else 0

    elements = variant_config.get("elements", "multiple elements")

    if total_anomalies > 0:
        summary = (
            f"Geochemical analysis of the {variant_config.get('study_area', 'study area')} "
            f"identified {total_anomalies} anomalous samples across {elements}. "
            f"These anomalies indicate potential geochemical enrichment zones "
            f"requiring further investigation."
        )
    else:
        summary = (
            f"Analysis of the {variant_config.get('study_area', 'study area')} "
            f"showed no significant geochemical anomalies in {elements}. "
            f"Results suggest relatively uniform geochemical distribution."
        )

    return summary
