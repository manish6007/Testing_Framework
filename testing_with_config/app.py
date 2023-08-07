import pandas as pd
from common.common_functions import *
import yaml


def read_aws_credentials():
    config = yaml.safe_load(open('config.yml'))
    aws_access_key_id = config['AWS']['aws_access_key_id']
    aws_secret_access_key = config['AWS']['aws_secret_access_key']
    return aws_access_key_id, aws_secret_access_key


def generate_comparison_report():
    config = yaml.safe_load(open('config.yml'))
    comparison_type = int(input("Please choose from comparison types below options \n"
                                "1. File to File comparison. \n "
                                "2. Oracle vs Athena datatype comparison. \n "
                                "3. Oracle vs Athena Field Name comparison. \n "
                                "4. Oracle vs Athena Data comparison. \n "
                                "5. Oracle vs Redshift datatype comparison. \n"))
    report_format = input("Please enter the format in which you want the report CSV/EXCEL/HTML: ")
    while True:
        if report_format.upper() in ["CSV", "EXCEL", "HTML"]:
            break
    report_location = config['Comparison']['report_location']

    aws_access_key_id, aws_secret_access_key = read_aws_credentials()

    ora_user = config['Comparison']['ora_user']
    ora_pwd = config['Comparison']['ora_pwd']
    ora_tab_name = config['Comparison']['ora_tab_name']
    host = config['Comparison']['host']
    port = config['Comparison']['port']
    service_name = config['Comparison']['service_name']
    region = config['Comparison']['region']
    athena_db = config['Comparison']['athena_db']
    athena_table = config['Comparison']['athena_table']
    output_location = config['Comparison']['output_location']
    file_path = config['Comparison']['file_path']
    redshift_user = config['Comparison']['redshift_user']
    redshift_pwd = config['Comparison']['redshift_pwd']
    redshift_tab_name = config['Comparison']['redshift_tab_name']
    redshift_host = config['Comparison']['redshift_host']
    redshift_port = config['Comparison']['redshift_port']
    redshift_db = config['Comparison']['redshift_db']

    if comparison_type == 1:

        file1 = input("Please enter first file name: ")
        file2 = input("Please enter second file name: ")
        df1 = pd.read_csv(f"{file_path}/{file1}", encoding='latin-1')
        df2 = pd.read_csv(f"{file_path}/{file2}", encoding='latin-1')

        df = compare_files(df1, df2)

    elif comparison_type == 2:
        differences = compare_oracle_vs_athena_data_types(ora_user, ora_pwd, host, port, service_name, ora_tab_name,
                                                          region, athena_db, athena_table)
        write_list_of_dicts_to_text(differences, f"{file_path}/Datatype_comparison_report.txt")

    elif comparison_type == 3:
        athena_field_names = get_athena_field_names(region, athena_db, athena_table)
        oracle_field_names = get_oracle_field_names(ora_user, ora_pwd, host, port,
                                                    service_name, ora_tab_name)
        df = compare_field_names(oracle_field_names, athena_field_names)

    elif comparison_type == 4:
        df_athena = create_dataframe_from_athena_table(region, athena_db, athena_table, output_location,
                                                       aws_access_key_id, aws_secret_access_key)
        df_oracle = read_oracle_table(ora_user, ora_pwd, host, port, service_name, ora_tab_name)
        df = compare_files(df_athena, df_oracle)

    elif comparison_type == 5:
        redshift_datatypes = redshift_to_oracle_datatypes(
            get_redshift_table_schema(redshift_host, redshift_host, redshift_db, redshift_user, redshift_pwd, redshift_tab_name))
        oracle_datatypes = convert_oracle_output_to_dict(
            get_oracle_columns_and_datatypes(ora_user, ora_pwd, host, port, service_name, ora_tab_name))
        df = compare_oracle_redshift_datatypes(redshift_datatypes, oracle_datatypes)
    else:
        print("Invalid comparison type.")
        return

    if comparison_type == 1 or comparison_type == 3 or comparison_type == 4 or comparison_type == 5:
        if comparison_type == 5:
            if report_format.upper() == "CSV":
                generate_csv_report(df, f"{report_location}/RS_VS_ORACLE_DTYPE_COMPARISON_{ora_tab_name}.csv")
            elif report_format.upper() == "Excel":
                generate_excel_report(df, f"{report_location}/RS_VS_ORACLE_DTYPE_COMPARISON_{ora_tab_name}.xlsx")
            elif report_format.upper() == "HTML":
                generate_html_report(df, f"{report_location}/RS_VS_ORACLE_DTYPE_COMPARISON_{ora_tab_name}.html")
            else:
                print("Invalid report format.")

        elif comparison_type == 3:
            if report_format.upper() == "CSV":
                generate_csv_report(df, f"{report_location}/ATHENA_VS_ORACLE_FIELDS_COMPARISON_{ora_tab_name}.csv")
            elif report_format.upper() == "Excel":
                generate_excel_report(df, f"{report_location}/ATHENA_VS_ORACLE_FIELDS_COMPARISON_{ora_tab_name}.xlsx")
            elif report_format.upper() == "HTML":
                generate_html_report(df, f"{report_location}/ATHENA_VS_ORACLE_FIELDS_COMPARISON_{ora_tab_name}.html")
            else:
                print("Invalid report format.")
        else:
            if report_format.upper() == "CSV":
                generate_csv_report(df, f"{report_location}/comparison_report_{ora_tab_name}.csv")
            elif report_format.upper() == "Excel":
                generate_excel_report(df, f"{report_location}/comparison_report_{ora_tab_name}.xlsx")
            elif report_format.upper() == "HTML":
                generate_html_report(df, f"{report_location}/comparison_report_{ora_tab_name}.html")
            else:
                print("Invalid report format.")

    print(f"The report generated successfully at {report_location}")


if __name__ == "__main__":
    generate_comparison_report()
