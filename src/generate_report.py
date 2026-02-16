#!/usr/bin/env python3
"""Generate DOCX validation report with CORRECTED NWM data."""

from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from datetime import datetime

def add_heading(doc, text, level=1):
    heading = doc.add_heading(text, level=level)
    return heading

def add_table(doc, headers, rows):
    table = doc.add_table(rows=1, cols=len(headers))
    table.style = 'Table Grid'
    
    hdr_cells = table.rows[0].cells
    for i, header in enumerate(headers):
        hdr_cells[i].text = header
        hdr_cells[i].paragraphs[0].runs[0].bold = True
    
    for row_data in rows:
        row_cells = table.add_row().cells
        for i, cell_data in enumerate(row_data):
            row_cells[i].text = str(cell_data)
    
    return table

def main():
    doc = Document()
    
    # Title
    title = doc.add_heading('Streamflow Model Validation Report', 0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    
    subtitle = doc.add_paragraph()
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = subtitle.add_run('HPP Neural Network Model vs NOAA National Water Model (NWM)')
    run.italic = True
    
    date_para = doc.add_paragraph()
    date_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    date_para.add_run(f'Test Date: July 15, 2024')
    date_para.add_run(f'\nReport Generated: {datetime.now().strftime("%B %d, %Y")}')
    
    doc.add_paragraph()
    
    # Executive Summary
    add_heading(doc, 'Executive Summary', 1)
    doc.add_paragraph(
        'This report presents a validation comparison between two streamflow prediction models: '
        'the HPP neural network ensemble model and NOAA\'s National Water Model (NWM). '
        'Both models were evaluated against observed streamflow data from USGS gauging stations '
        'across three states: Texas, California, and North Carolina.'
    )
    doc.add_paragraph(
        'Key Findings:\n'
        '• NWM significantly outperforms HPP in overall accuracy (NSE 0.718 vs 0.245)\n'
        '• NWM achieves excellent correlation (R² = 0.786) with low bias (+11.4%)\n'
        '• HPP consistently underestimates flows by approximately 49%\n'
        '• Both models perform best in North Carolina; NWM excels in California\n'
        '• HPP shows competitive performance in NC with near-zero bias (-4.7%)'
    )
    
    # Methodology
    add_heading(doc, 'Methodology', 1)
    
    add_heading(doc, 'Data Sources', 2)
    doc.add_paragraph(
        '• HPP Model: Neural network ensemble (10 models) trained on watersheds up to 75,000 km². '
        'Outputs include median prediction (q50) and uncertainty bounds (q25, q75) in cubic feet per second (CFS).\n'
        '• NWM: NOAA National Water Model Analysis and Assimilation output for July 15, 2024, '
        'downloaded from Google Cloud Storage (gs://national-water-model/), converted from m³/s to CFS.\n'
        '• USGS Observed: Daily mean streamflow values from USGS Water Services API for active gauging stations.'
    )
    
    add_heading(doc, 'Test Configuration', 2)
    doc.add_paragraph(
        '• Test Date: July 15, 2024 (representative summer operational date)\n'
        '• Geographic Coverage: Texas (TX), California (CA), North Carolina (NC)\n'
        '• Total USGS Stations Evaluated: 1,129\n'
        '• Stations with Valid HPP Comparisons: 960\n'
        '• Stations with Valid NWM Comparisons: 901'
    )
    
    add_heading(doc, 'Model-to-USGS Site Matching', 2)
    
    add_heading(doc, 'HPP Model Matching', 3)
    doc.add_paragraph(
        'The HPP model predictions were matched to USGS sites using a direct identifier linkage:\n\n'
        '• The HPP parquet file uses a UUID as the primary identifier for each prediction location\n'
        '• The accompanying pour_points.geojson file (provided by the HPP vendor) contains the mapping '
        'between UUID and USGS site_id\n'
        '• For sites with USGS gauges, the UUID is the USGS site identifier '
        '(e.g., UUID "11152650" corresponds to USGS site 11152650)\n\n'
        'This represents a clean 1:1 match because the HPP model was specifically trained and run '
        'for these exact USGS gauge locations.'
    )
    
    add_heading(doc, 'NWM Model Matching', 3)
    doc.add_paragraph(
        'The NWM outputs predictions by COMID (NHD+ reach identifier), not by USGS site. '
        'A spatial join was performed to link USGS gauges to their underlying river reaches:\n\n'
        '1. For each USGS gauge location, query all NHD+ river reaches within approximately 1 km\n'
        '2. Select the nearest reach based on geometric distance\n'
        '3. Retrieve the NWM streamflow prediction for that reach\'s COMID from the '
        'July 15, 2024 Analysis and Assimilation output (t12z)\n\n'
        'NWM data source: gs://national-water-model/nwm.20240715/analysis_assim/nwm.t12z.analysis_assim.channel_rt.tm00.conus.nc'
    )
    
    add_heading(doc, 'Matching Confidence Assessment', 3)
    headers = ['Aspect', 'Confidence', 'Notes']
    rows = [
        ['HPP ↔ USGS matching', 'High', 'Direct UUID = site_id mapping from vendor'],
        ['NWM ↔ USGS matching', 'Moderate', 'Spatial join within ~1km; some mismatches possible'],
        ['NWM date alignment', 'High', 'Actual July 15, 2024 data from GCS'],
        ['USGS data quality', 'High', 'Official daily values from USGS API'],
    ]
    add_table(doc, headers, rows)
    
    doc.add_paragraph()
    
    # Metrics Explanation
    add_heading(doc, 'Validation Metrics Explained', 1)
    
    add_heading(doc, 'Nash-Sutcliffe Efficiency (NSE)', 2)
    doc.add_paragraph(
        'NSE measures how well the model predictions match observed values compared to simply using the mean of observations. '
        'It ranges from -∞ to 1, where:\n'
        '• NSE = 1: Perfect match\n'
        '• NSE = 0: Model performs as well as using the observed mean\n'
        '• NSE < 0: Model performs worse than using the observed mean\n\n'
        'Interpretation Guidelines:\n'
        '• NSE > 0.75: Very good\n'
        '• 0.65 < NSE ≤ 0.75: Good\n'
        '• 0.50 < NSE ≤ 0.65: Satisfactory\n'
        '• NSE ≤ 0.50: Unsatisfactory'
    )
    
    add_heading(doc, 'Coefficient of Determination (R²)', 2)
    doc.add_paragraph(
        'R² measures the proportion of variance in observed values that is explained by the model. '
        'It ranges from 0 to 1, where:\n'
        '• R² = 1: Model explains all variability\n'
        '• R² = 0: Model explains no variability\n\n'
        'R² indicates correlation strength but does not account for systematic bias.'
    )
    
    add_heading(doc, 'Percent Bias (PBIAS)', 2)
    doc.add_paragraph(
        'PBIAS measures the average tendency of predictions to be larger or smaller than observed values:\n'
        '• PBIAS = 0%: No systematic bias\n'
        '• PBIAS < 0%: Model underestimates (negative bias)\n'
        '• PBIAS > 0%: Model overestimates (positive bias)\n\n'
        'Interpretation Guidelines:\n'
        '• |PBIAS| < 10%: Very good\n'
        '• 10% ≤ |PBIAS| < 25%: Good\n'
        '• 25% ≤ |PBIAS| < 40%: Satisfactory\n'
        '• |PBIAS| ≥ 40%: Unsatisfactory'
    )
    
    add_heading(doc, 'Log-transformed NSE (Log-NSE)', 2)
    doc.add_paragraph(
        'Log-NSE is calculated using log-transformed flow values. This metric:\n'
        '• Reduces the influence of high flows on the overall score\n'
        '• Better evaluates model performance across the full range of flows\n'
        '• Is particularly useful for assessing low-flow and drought conditions'
    )
    
    # Results
    add_heading(doc, 'Validation Results', 1)
    
    add_heading(doc, 'Overall Performance (All States Combined)', 2)
    
    headers = ['Metric', 'HPP vs USGS', 'NWM vs USGS', 'Better Model']
    rows = [
        ['Sample Size (n)', '960', '901', '—'],
        ['NSE', '0.245', '0.718', 'NWM ✓'],
        ['R²', '0.288', '0.786', 'NWM ✓'],
        ['PBIAS', '-49.3%', '+11.4%', 'NWM ✓'],
        ['Log-NSE', '0.513', '0.867', 'NWM ✓'],
    ]
    add_table(doc, headers, rows)
    
    doc.add_paragraph()
    doc.add_paragraph(
        'NWM significantly outperforms HPP across all metrics. NWM achieves good NSE (0.718), '
        'strong correlation (R² = 0.786), and relatively low bias (+11.4% overestimation). '
        'HPP shows unsatisfactory NSE (0.245) and substantial underestimation bias (-49.3%).'
    )
    
    # Texas
    add_heading(doc, 'Texas (TX)', 2)
    headers = ['Metric', 'HPP', 'NWM', 'Better Model']
    rows = [
        ['Sample Size (n)', '392', '370', '—'],
        ['NSE', '0.255', '0.662', 'NWM ✓'],
        ['R²', '0.316', '0.763', 'NWM ✓'],
        ['PBIAS', '-58.4%', '+19.2%', 'NWM ✓'],
        ['Log-NSE', '0.397', '0.859', 'NWM ✓'],
    ]
    add_table(doc, headers, rows)
    
    doc.add_paragraph()
    doc.add_paragraph(
        'NWM substantially outperforms HPP in Texas, achieving good NSE (0.662) compared to '
        'HPP\'s unsatisfactory 0.255. NWM shows strong correlation (R² = 0.763) with moderate '
        'overestimation (+19.2%), while HPP severely underestimates flows (-58.4%).'
    )
    
    # California
    add_heading(doc, 'California (CA)', 2)
    headers = ['Metric', 'HPP', 'NWM', 'Better Model']
    rows = [
        ['Sample Size (n)', '336', '310', '—'],
        ['NSE', '0.124', '0.879', 'NWM ✓'],
        ['R²', '0.145', '0.892', 'NWM ✓'],
        ['PBIAS', '-47.2%', '+7.2%', 'NWM ✓'],
        ['Log-NSE', '0.571', '0.841', 'NWM ✓'],
    ]
    add_table(doc, headers, rows)
    
    doc.add_paragraph()
    doc.add_paragraph(
        'NWM achieves its best performance in California with very good NSE (0.879) and '
        'excellent correlation (R² = 0.892). NWM bias is very good at only +7.2%. '
        'HPP performs poorly here (NSE = 0.124) with substantial underestimation (-47.2%). '
        'California\'s snowmelt-driven hydrology appears better captured by NWM\'s physics-based approach.'
    )
    
    # North Carolina
    add_heading(doc, 'North Carolina (NC)', 2)
    headers = ['Metric', 'HPP', 'NWM', 'Better Model']
    rows = [
        ['Sample Size (n)', '232', '221', '—'],
        ['NSE', '0.617', '0.638', 'NWM (slight)'],
        ['R²', '0.632', '0.661', 'NWM (slight)'],
        ['PBIAS', '-4.7%', '-20.6%', 'HPP ✓'],
        ['Log-NSE', '0.656', '0.925', 'NWM ✓'],
    ]
    add_table(doc, headers, rows)
    
    doc.add_paragraph()
    doc.add_paragraph(
        'North Carolina shows the closest competition between models. Both achieve satisfactory '
        'NSE scores (HPP: 0.617, NWM: 0.638). HPP\'s key strength here is its near-zero bias '
        '(-4.7%), which is very good, compared to NWM\'s good bias of -20.6%. '
        'NWM achieves excellent Log-NSE (0.925), indicating superior low-flow prediction. '
        'For applications requiring unbiased estimates, HPP may be preferred in this region.'
    )
    
    # Summary Table
    add_heading(doc, 'State Performance Summary', 2)
    headers = ['State', 'HPP Metrics Won', 'NWM Metrics Won', 'Recommended Model']
    rows = [
        ['Texas', '0', '4', 'NWM'],
        ['California', '0', '4', 'NWM'],
        ['North Carolina', '1 (PBIAS)', '3', 'NWM (or HPP for low-bias needs)'],
    ]
    add_table(doc, headers, rows)
    
    # Conclusions
    add_heading(doc, 'Conclusions and Recommendations', 1)
    
    doc.add_paragraph(
        '1. NWM Outperforms HPP Overall: NWM demonstrates substantially better accuracy across '
        'all states and metrics, with good-to-very-good NSE scores (0.662–0.879) compared to '
        'HPP\'s unsatisfactory-to-satisfactory scores (0.124–0.617).\n\n'
        '2. HPP Bias Issue: HPP consistently underestimates streamflow by approximately 47-58% '
        'in Texas and California. This systematic negative bias limits its utility for '
        'absolute flow estimation.\n\n'
        '3. HPP Strength in NC: HPP achieves its best performance in North Carolina with '
        'near-zero bias (-4.7%), making it potentially useful for applications where '
        'unbiased estimates are critical.\n\n'
        '4. NWM California Performance: NWM excels in California (NSE = 0.879, R² = 0.892), '
        'likely due to better representation of snowmelt-driven western hydrology in the '
        'physics-based model.\n\n'
        '5. Recommendations:\n'
        '   • For operational streamflow estimation: Use NWM\n'
        '   • For drought/flood classification in NC: HPP may be suitable due to low bias\n'
        '   • For western US applications: Strongly prefer NWM\n'
        '   • HPP may benefit from regional bias correction to improve absolute accuracy'
    )
    
    # Appendix
    add_heading(doc, 'Appendix: Data Sources and Files', 1)
    doc.add_paragraph(
        'Data files generated during this analysis:\n\n'
        '• state_comparison_fixed.csv: Full comparison dataset with HPP, USGS, and NWM values\n'
        '• state_metrics_fixed.csv: Summary metrics by state and model\n'
        '• nwm_20240715_12z.parquet: NWM streamflow data for July 15, 2024\n\n'
        'NWM Data Source:\n'
        '• Google Cloud Storage: gs://national-water-model/nwm.20240715/\n'
        '• Product: Analysis and Assimilation (analysis_assim)\n'
        '• File: nwm.t12z.analysis_assim.channel_rt.tm00.conus.nc\n'
        '• Reaches: 2,709,580 COMIDs with valid streamflow'
    )
    
    # Save
    output_path = 'results/HPP_NWM_Validation_Report.docx'
    doc.save(output_path)
    print(f"Report saved to: {output_path}")

if __name__ == '__main__':
    main()
