# Testing Framework for Data Comparison

This framework serves as a comprehensive solution for comparing datatypes, field names, and data among Oracle, Athena, and Redshift databases, while also facilitating file-to-file comparisons. It accommodates the needs of both single and multiple table comparisons and boasts a flexible feature set for generating diverse styles of comparison reports.

## Prerequisites

Before using the testing framework, ensure you have the following prerequisites in place:

- Python 3.x
- Required Python packages: `pandas`, `yaml`
- AWS credentials configured with appropriate permissions
- Configuration file (`config.yml`) containing necessary connection and configuration details

## Setup and Usage

1. Clone the repository and navigate to the root directory.

2. Configure the `config.yml` file with your AWS credentials, database connection details, and other required parameters.

3. Run the `generate_comparison_report.py` script using the following command:

    ```
    python generate_comparison_report.py
    ```

4. Choose a comparison type from the available options:

    - **1. File to File comparison**
    - **2. Oracle vs Athena datatype comparison**
    - **3. Oracle vs Athena Field Name comparison**
    - **4. Oracle vs Athena Data comparison**
    - **5. Oracle vs Redshift datatype comparison**
    - **6. Oracle vs Redshift Field Name comparison**

5. Specify the desired report format (CSV/EXCEL/HTML) for the generated comparison report.

6. Follow the on-screen prompts based on the selected comparison type. Provide required inputs such as file names, table names, etc.

7. Once the comparison is completed, the script will generate a comparison report in the chosen format and save it to the specified report location.

## Report Generation

The framework provides the flexibility to generate comparison reports in different formats based on the selected comparison type. The generated reports will be named appropriately, including relevant details like table names and comparison types.

### Supported Report Formats

- **CSV**: Comma-separated values format.
- **Excel**: Excel spreadsheet format.
- **HTML**: HTML format for web-based viewing.

## Conclusion

This testing framework streamlines the process of comparing datatypes, field names, and data between different database platforms and conducting file-to-file comparisons. By leveraging this framework, you can ensure data consistency and integrity across various sources, aiding in quality assurance efforts.
