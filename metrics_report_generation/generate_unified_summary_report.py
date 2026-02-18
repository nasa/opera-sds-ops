import argparse
import csv
import datetime
import glob
import io
import os.path as osp
import statistics

from matplotlib.axes import Axes
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


RT_SUM_HEADER = ['# Title: OPERA Retrieval Time Summary',
                 '# Date of Report: {gen_datetime_utc}',
                 '# Period of Coverage (AcquisitionTime): {cov_start} - {cov_end}',
]

RT_DET_HEADER = ['# Title: OPERA Retrieval Time Log',
                 '# Date of Report: {gen_datetime_utc}',
                 '# Period of Coverage (AcquisitionTime): {cov_start} - {cov_end}',
                 '# PublicAvailableDateTime: datetime when the product was first made available to the public by the DAAC.',
                 '# OperaDetectDateTime: datetime when the OPERA system first became aware of the product.',
                 '# ProductReceivedDateTime: datetime when the product arrived in our system',
]

RT_COLS = ['OPERA Product Short Name',
           'Input Product Short Name',
	       'Retrieval Time (count)',
	       'Retrieval Time (P90)',
	       'Retrieval Time (min)',
	       'Retrieval Time (max)',
	       'Retrieval Time (median)',
	       'Retrieval Time (mean)',
	       'Histogram',
]

PT_SUM_HEADER = ['# Title: OPERA Production Time Summary',
                 '# Date of Report: {gen_datetime_utc}',
                 '# Period of Coverage (AcquisitionTime): {cov_start} - {cov_end}',
]

PT_DET_HEADER = ['Title: OPERA Production Time Log',
                 'Date of Report: {gen_datetime_utc}',
                 'Period of Coverage (AcquisitionTime): {cov_start} - {cov_end}',
]

PT_COLS = ['OPERA Product Short Name',
	       'Production Time (count)',
	       'Production Time (min)',
	       'Production Time (max)',
	       'Production Time (mean)',
	       'Production Time (median)',
]


# TODO:  would be good to have this able to deconflict lines it reads from CSV
#        files.  That would make it robust against files having overlapping
#        coverages (e.g. mixing weekly reports with daily reports, or if a daily
#        report gets double-loaded somehow).
def read_all_csv_files(all_files, skip=0):
    """Read all CSV files in `all_files`, and combine into a single dataframe.

    Args:
        all_files (list): List of strings, where each is a path to a CSV file to ingest.
        skip (int): Number of rows to skip while reading the CSV files.

    Returns:
        df (pd.DataFrame): Concatenated DataFrame containing data from all CSV files.
        start (datetime.datetime): Earliest date and time among all files.
        end (datetime.datetime): Latest date and time among all files.
    """
    df = pd.DataFrame()

    start = datetime.datetime(3000, 1, 1)
    end = datetime.datetime(2000, 1, 1)

    for csvfile in all_files:

        # grab the start and end date from the filename
        # Note:  this is specialized to our current filename convention
        t1, t2 = osp.splitext(csvfile)[0].split(' ')[2::2]
        start = min(start, datetime.datetime.strptime(t1, '%Y-%m-%dT%H%M%S'))
        end = max(end, datetime.datetime.strptime(t2, '%Y-%m-%dT%H%M%S'))
        
        df_temp = pd.DataFrame()

        try:
            df_temp = pd.read_csv(csvfile, skiprows=skip)
            print(csvfile)
        except:
            print("CSV file was empty or had an error reading: ", csvfile)

        df = pd.concat((df, df_temp), ignore_index=True)

                
    return df, start, end



# This function is lifted from PCM's report_util.py.  Docstring supplied by ChatGPT
# https://github.com/nasa/opera-sds-bach-api/blob/188311866572a7d32a57e4aebd7cb443e05aa2a1/accountability_api/api_utils/reporting/report_util.py#L11
def to_duration_isoformat(duration_seconds: float):
    """
    Converts a duration in seconds to ISO 8601 formatted time duration.

    Args:
        duration_seconds (float): The duration in seconds.

    Returns:
        str: The duration formatted in the ISO 8601 format (HH:MM:SS).
    """
    td: pd.Timedelta = pd.Timedelta(f'{int(duration_seconds)} s')
    hh = 24 * td.components.days + td.components.hours
    hhmmss_format = f"{hh:02d}:{td.components.minutes:02d}:{td.components.seconds:02d}"
    return hhmmss_format


def get_retrieval_time_histogram_filename(start_datetime, end_datetime, sds_product_name, input_product_name, report_type='summary', outdir='.'):
    """
    Generates a filename for the retrieval time histogram image.

    Args:
        start_datetime (str): The start datetime of the retrieval time range.
        end_datetime (str): The end datetime of the retrieval time range.
        sds_product_name (str): The name of the SDS product.
        input_product_name (str): The name of the input product.
        report_type (str, optional): The type of report. Defaults to 'summary'.
        outdir (str, optional): The directory for writing output.  Defaults to '.'.

    Returns:
        str: The filename for the retrieval time histogram image.
    """
    start_datetime_normalized = start_datetime.replace(":", "")
    end_datetime_normalized = end_datetime.replace(":", "")

    return osp.join(outdir, f"retrieval-time-{report_type} - {sds_product_name} - {input_product_name} - {start_datetime_normalized} to {end_datetime_normalized}.png")


def generate_retrieval_time_histogram(sub_df, start, end, outputs, inputs, column='Retrieval Time', outdir='.'):
    """
    Generates a retrieval time histogram and saves it as an image file.

    Args:
        sub_df (pandas.DataFrame): The subset of data for generating the histogram.
        start (str): The start datetime of the retrieval time range.
        end (str): The end datetime of the retrieval time range.
        outputs (str): The name of the outputs.
        inputs (str): The name of the inputs.
        column (str, optional): The column name in the DataFrame to plot. Defaults to 'Retrieval Time'.
        outdir (str, optional): The directory for writing output.  Defaults to '.'.

    Returns:
        str: The path to the generated histogram image file.
    """
    title = f'{inputs} {column} for {outputs} inputs'

    histogram_name = get_retrieval_time_histogram_filename(start,
                                                           end,
                                                           outputs,
                                                           inputs,
                                                           outdir=outdir,
    )

    return generate_histogram(sub_df, start, end, outputs, column=column, title=title, histogram_name=histogram_name)
    

def get_production_time_histogram_filename(start_datetime, end_datetime, sds_product_name, report_type='summary', outdir='.'):
    """
    Generates a filename for the production time histogram image.

    Args:
        start_datetime (str): The start datetime of the retrieval time range.
        end_datetime (str): The end datetime of the retrieval time range.
        sds_product_name (str): The name of the SDS product.
        report_type (str, optional): The type of report. Defaults to 'summary'.
        outdir (str, optional): The directory for writing output.  Defaults to '.'.

    Returns:
        str: The filename for the production time histogram image.
    """
    start_datetime_normalized = start_datetime.replace(":", "")
    end_datetime_normalized = end_datetime.replace(":", "")

    return osp.join(outdir, f"production-time-{report_type} - {sds_product_name} - {start_datetime_normalized} to {end_datetime_normalized}.png")


def generate_production_time_histogram(sub_df, start, end, outputs, column='Production Time', outdir='.'):
    """
    Generates a production time histogram and saves it as an image file.

    Args:
        sub_df (pandas.DataFrame): The subset of data for generating the histogram.
        start (str): The start datetime of the retrieval time range.
        end (str): The end datetime of the retrieval time range.
        outputs (str): The name of the outputs.
        column (str, optional): The column name in the DataFrame to plot. Defaults to 'Production Time'.
        outdir (str, optional): The directory for writing output.  Defaults to '.'.

    Returns:
        str: The path to the generated histogram image file.
    """

    title = f'{outputs} {column}'

    histogram_name = get_production_time_histogram_filename(start,
                                                            end,
                                                            outputs,
                                                            outdir=outdir,
    )

    return generate_histogram(sub_df, start, end, outputs, column=column, title=title, histogram_name=histogram_name)


def generate_histogram(sub_df, start, end, outputs, column, title, histogram_name):
    """
    Generates a histogram plot based on the provided data and saves it as an image file.

    Args:
        sub_df (pandas.DataFrame): The subset of data used for generating the histogram.
        start (str): The start datetime of the data range.
        end (str): The end datetime of the data range.
        outputs (str): The name of the outputs.
        column (str): The column name in the DataFrame to plot.
        title (str): The title of the histogram plot.
        histogram_name (str): The filename to save the histogram image.

    Returns:
        str: The path to the generated histogram image file.
    """

    # convert the retrieval times into decimal hours right up front.
    hours = pd.to_timedelta(sub_df[column]).apply(pd.Timedelta.total_seconds) / 60 / 60

    hours_min = np.min(hours)
    hours_max = np.max(hours)
    hours_p90 = np.percentile(hours, 90)
    hours_avg = np.mean(hours)

    if column == 'Retrieval Time':
        hours_plot_stat = hours_p90
    elif column == 'Production Time':
        hours_plot_stat = hours_avg
    else:
        raise ValueError(f'Bad value for `column` input.  Value = {column}')
    
    #xticks = [hours_min, hours_p90, hours_max, 0, 24, 30]
    #xticklabels = [f"{x:.2f}" for x in xticks]
    #xticklabels = [f"{hours_min:.1f}", f"{hours_p90:.2f}", f"{hours_max:.1f}", '0', '24', '30+']

    xticks = [hours_min]
    xticklabels = [f"{hours_min:.1f}"]
    if hours_plot_stat < 30:
        xticks += [hours_plot_stat]
    else:
        xticks += [30]
        pass
    xticklabels += [f"{hours_plot_stat:.2f}"]

    if hours_max < 30:
        xticks += [hours_max]
        xticklabels += [f"{hours_max:.1f}"]
    else:
        xticks += [30]
        xticklabels += ['30+']
        pass

    if hours_max > 24:
        xticks += [24]
        xticklabels += ['24']

    
    bins = np.arange(0, 30.25, 0.25)
    
    print('plotting histogram')
    plt.figure()
    # Plot the histogram
    #plt.hist(retrieval_time_hours, bins='auto', color='steelblue', edgecolor='black')
    plt.hist(np.clip(np.array(hours), 0, 30), bins=200)

    # Add vertical lines and labels for statistics
    line_color = 'k'
    if hours_plot_stat > 24:
        line_color = 'r'
        pass
    line_xloc = hours_plot_stat
    if hours_plot_stat > 30:
        line_xloc = 30
        pass
    plt.axvline(x=line_xloc, color=line_color, linestyle='dashed', linewidth=1, alpha=0.5)

    # Set plot title and labels
    plt.title(title)
    plt.xlabel(f'{column} (hours)')
    plt.yticks([])
    plt.xticks(xticks, xticklabels)

    plt.tight_layout()
    #print('showing histogram')
    #plt.show()

    print(histogram_name)
    plt.savefig(histogram_name)

    return histogram_name


def write_report_to_csv(df, csvfile, metadata):
    """
    Writes the provided DataFrame and metadata to a CSV file.

    Args:
        df (pandas.DataFrame): The DataFrame to be written to the CSV file.
        csvfile (str): The path or filename of the CSV file.
        metadata (list): A list of strings representing the metadata information.

    Returns:
        None
    """
    # Write the metadata and dataframe to a CSV file
    with open(csvfile, 'w') as f:
        f.write('\n'.join(metadata) + '\n')
        df.to_csv(f, index=False)

    return


def calculate_P90(sub_df, column='Retrieval Time'):
    return pd.Timedelta(np.percentile(pd.to_timedelta(sub_df[column]), 90)).to_pytimedelta()

def calculate_min(sub_df, column='Retrieval Time'):
    return pd.Timedelta(np.min(pd.to_timedelta(sub_df[column]))).to_pytimedelta()

def calculate_max(sub_df, column='Retrieval Time'):
    return pd.Timedelta(np.max(pd.to_timedelta(sub_df[column]))).to_pytimedelta()

def calculate_median(sub_df, column='Retrieval Time'):
    return pd.Timedelta(np.median(pd.to_timedelta(sub_df[column]))).to_pytimedelta()

def calculate_mean(sub_df, column='Retrieval Time'):
    return pd.Timedelta(np.mean(pd.to_timedelta(sub_df[column]))).to_pytimedelta()



def generate_retrieval_time_report(input_list, column='Retrieval Time', skip=0, save_unified_detailed_report=True, outdir='.'):
    """
    Generates a retrieval time summary report based on detailed retrieval time reports identified by the `input_list`.

    Args:
        input_list (list): List of strings, where each is a path to a CSV file to ingest.
        column (str, optional): The column name representing the retrieval time. Defaults to 'Retrieval Time'.
        skip (int, optional): The number of rows to skip when reading the CSV files. Defaults to 0.
        save_unified_detailed_report (bool, optional): Flag indicating whether to save the detailed retrieval time report.
            Defaults to True.
        outdir (str, optional): The directory for writing output.  Defaults to '.'.

    Returns:
        pandas.DataFrame: The summary DataFrame containing retrieval time statistics.

    """
    df, start, end = read_all_csv_files(input_list, skip)

    gen_datetime_utc = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    fname_format_str = '%Y-%m-%dT%H%M%S'
    metadata_format_str = '%Y-%m-%dT%H:%M:%SZ'

    if save_unified_detailed_report:
        # Write out the detailed retrieval time report, the union of all the ones ingested.
        csvfname_detailed = osp.join(outdir, f'retrieval-time-detailed - {start.strftime(fname_format_str)} to {end.strftime(fname_format_str)}.csv')
        metadata_detailed = [s.format(gen_datetime_utc=gen_datetime_utc,
                                      cov_start=start.strftime(metadata_format_str),
                                      cov_end=end.strftime(metadata_format_str),
        ) for s in RT_DET_HEADER]
        print(csvfname_detailed)
        print(metadata_detailed)
        write_report_to_csv(df, csvfname_detailed, metadata_detailed)
        pass

    #print(df)
    #print(start, end)
    opera_product_short_names = list(np.unique(df['OPERA Product Short Name']))

    summary_categories = dict.fromkeys(opera_product_short_names)

    for key, item in summary_categories.items():
        input_product_types = list(np.unique(df['Input Product Type']))
        if len(input_product_types) > 1:
            input_product_types = input_product_types + ['ALL']
            pass

        summary_categories[key] = input_product_types

        pass
    # The loop above should produce a dict like below.
    #summary_categories = {'L3_DSWx_HLS': ['L2_HLS_L30', 'L2_HLS_S30', 'ALL'],
    #}

    # Init the dataframe for the output
    # TODO:  make code to populate this based on the results of the above loop.
    df_summary = pd.DataFrame({'OPERA Product Short Name': ['L3_DSWx_HLS', 'L3_DSWx_HLS', 'L3_DSWx_HLS'],
                               'Input Product Short Name': ['L2_HLS_L30', 'L2_HLS_S30', 'ALL'],
                               'Retrieval Time (count)': [None, None, None],
                               'Retrieval Time (P90)': [None, None, None],
                               'Retrieval Time (min)': [None, None, None],
                               'Retrieval Time (max)': [None, None, None],
                               'Retrieval Time (median)': [None, None, None],
                               'Retrieval Time (mean)': [None, None, None],
                               'Histogram': [None, None, None],
    })

    #inds_L30 = (df['OPERA Product Short Name'] == 'L3_DSWx_HLS') & (df['Input Product Type'] == 'L2_HLS_L30')
    #inds_S30 = (df['OPERA Product Short Name'] == 'L3_DSWx_HLS') & (df['Input Product Type'] == 'L2_HLS_S30')
    #inds_ALL = (df['OPERA Product Short Name'] == 'L3_DSWx_HLS')

    # TODO:  change the code below into a proper loop
    sub_df_in = df.loc[(df['OPERA Product Short Name'] == 'L3_DSWx_HLS') & (df['Input Product Type'] == 'L2_HLS_L30')]
    sub_df_p90 = calculate_P90(   sub_df_in, column).total_seconds()
    sub_df_min = calculate_min(   sub_df_in, column).total_seconds()
    sub_df_max = calculate_max(   sub_df_in, column).total_seconds()
    sub_df_med = calculate_median(sub_df_in, column).total_seconds()
    sub_df_avg = calculate_mean(  sub_df_in, column).total_seconds()

    print(sub_df_in.shape[0])
    ind_summary = (df_summary['OPERA Product Short Name'] == 'L3_DSWx_HLS') & (df_summary['Input Product Short Name'] == 'L2_HLS_L30')
    df_summary.loc[ind_summary, 'Retrieval Time (count)'] = sub_df_in.shape[0]
    df_summary.loc[ind_summary, 'Retrieval Time (P90)'] = to_duration_isoformat(   sub_df_p90)
    df_summary.loc[ind_summary, 'Retrieval Time (min)'] = to_duration_isoformat(   sub_df_min)
    df_summary.loc[ind_summary, 'Retrieval Time (max)'] = to_duration_isoformat(   sub_df_max)
    df_summary.loc[ind_summary, 'Retrieval Time (median)'] = to_duration_isoformat(sub_df_med)
    df_summary.loc[ind_summary, 'Retrieval Time (mean)'] = to_duration_isoformat(  sub_df_avg)
    df_summary.loc[ind_summary, 'Histogram'] = generate_retrieval_time_histogram(sub_df_in,
                                                                                 start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                                                 end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                                                 'L3_DSWx_HLS',
                                                                                 'L2_HLS_L30',
                                                                                 column='Retrieval Time',
                                                                                 outdir=outdir,
    )
    
    sub_df_in = df.loc[(df['OPERA Product Short Name'] == 'L3_DSWx_HLS') & (df['Input Product Type'] == 'L2_HLS_S30')]
    sub_df_p90 = calculate_P90(   sub_df_in, column).total_seconds()
    sub_df_min = calculate_min(   sub_df_in, column).total_seconds()
    sub_df_max = calculate_max(   sub_df_in, column).total_seconds()
    sub_df_med = calculate_median(sub_df_in, column).total_seconds()
    sub_df_avg = calculate_mean(  sub_df_in, column).total_seconds()

    ind_summary = (df_summary['OPERA Product Short Name'] == 'L3_DSWx_HLS') & (df_summary['Input Product Short Name'] == 'L2_HLS_S30')
    df_summary.loc[ind_summary, 'Retrieval Time (count)'] = sub_df_in.shape[0]
    df_summary.loc[ind_summary, 'Retrieval Time (P90)'] = to_duration_isoformat(   sub_df_p90)
    df_summary.loc[ind_summary, 'Retrieval Time (min)'] = to_duration_isoformat(   sub_df_min)
    df_summary.loc[ind_summary, 'Retrieval Time (max)'] = to_duration_isoformat(   sub_df_max)
    df_summary.loc[ind_summary, 'Retrieval Time (median)'] = to_duration_isoformat(sub_df_med)
    df_summary.loc[ind_summary, 'Retrieval Time (mean)'] = to_duration_isoformat(  sub_df_avg)
    df_summary.loc[ind_summary, 'Histogram'] = generate_retrieval_time_histogram(sub_df_in,
                                                                                 start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                                                 end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                                                 'L3_DSWx_HLS',
                                                                                 'L2_HLS_S30',
                                                                                 column='Retrieval Time',
                                                                                 outdir=outdir,
    )
    
    sub_df_in = df.loc[(df['OPERA Product Short Name'] == 'L3_DSWx_HLS')]
    sub_df_p90 = calculate_P90(   sub_df_in, column).total_seconds()
    sub_df_min = calculate_min(   sub_df_in, column).total_seconds()
    sub_df_max = calculate_max(   sub_df_in, column).total_seconds()
    sub_df_med = calculate_median(sub_df_in, column).total_seconds()
    sub_df_avg = calculate_mean(  sub_df_in, column).total_seconds()

    ind_summary = (df_summary['OPERA Product Short Name'] == 'L3_DSWx_HLS') & (df_summary['Input Product Short Name'] == 'ALL')
    df_summary.loc[ind_summary, 'Retrieval Time (count)'] = sub_df_in.shape[0]
    df_summary.loc[ind_summary, 'Retrieval Time (P90)'] = to_duration_isoformat(   sub_df_p90)
    df_summary.loc[ind_summary, 'Retrieval Time (min)'] = to_duration_isoformat(   sub_df_min)
    df_summary.loc[ind_summary, 'Retrieval Time (max)'] = to_duration_isoformat(   sub_df_max)
    df_summary.loc[ind_summary, 'Retrieval Time (median)'] = to_duration_isoformat(sub_df_med)
    df_summary.loc[ind_summary, 'Retrieval Time (mean)'] = to_duration_isoformat(  sub_df_avg)
    df_summary.loc[ind_summary, 'Histogram'] = generate_retrieval_time_histogram(sub_df_in,
                                                                                 start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                                                 end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                                                 'L3_DSWx_HLS',
                                                                                 'ALL',
                                                                                 column='Retrieval Time',
                                                                                 outdir=outdir,
    )

    
    # This goes in the metadata header of the CSV
    csvfname_summary = osp.join(outdir, f'retrieval-time-summary - {start.strftime(fname_format_str)} to {end.strftime(fname_format_str)}.csv')
    #metadata_summary = RT_SUM_HEADER.format(gen_datetime_utc=gen_datetime_utc,
    #                                        cov_start=start.strftime(metadata_format_str),
    #                                        cov_end=end.strftime(metadata_format_str),
    #)
    metadata_summary = [s.format(gen_datetime_utc=gen_datetime_utc,
                                 cov_start=start.strftime(metadata_format_str),
                                 cov_end=end.strftime(metadata_format_str),
    ) for s in RT_SUM_HEADER]

    print(csvfname_summary)
    print(metadata_summary)
    write_report_to_csv(df_summary, csvfname_summary, metadata_summary)
    #df_summary.to_csv(csvfname_summary)

    
    return df_summary


# TODO:  this function is essentially a copy of the one above.  Probably a way to consolidate without too much complication/abstraction.
def generate_production_time_report(input_list, column='Production Time', skip=0, save_unified_detailed_report=True, outdir='.'):
    """
    Generates a production time summary report based on detailed production time reports identified by the `input_list`.

    Args:
        input_list (list): List of strings, where each is a path to a CSV file to ingest.
        column (str, optional): The column name representing the production time. Defaults to 'Production Time'.
        skip (int, optional): The number of rows to skip when reading the CSV files. Defaults to 0.
        save_unified_detailed_report (bool, optional): Flag indicating whether to save the detailed retrieval time report.
            Defaults to True.
        outdir (str, optional): The directory for writing output.  Defaults to '.'.

    Returns:
        pandas.DataFrame: The summary DataFrame containing retrieval time statistics.

    """
    df, start, end = read_all_csv_files(input_list, skip)

    gen_datetime_utc = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    fname_format_str = '%Y-%m-%dT%H%M%S'
    metadata_format_str = '%Y-%m-%dT%H:%M:%SZ'

    if save_unified_detailed_report:
        # Write out the detailed retrieval time report, the union of all the ones ingested.
        csvfname_detailed = osp.join(outdir, f'production-time-detailed - {start.strftime(fname_format_str)} to {end.strftime(fname_format_str)}.csv')
        metadata_detailed = [s.format(gen_datetime_utc=gen_datetime_utc,
                                      cov_start=start.strftime(metadata_format_str),
                                      cov_end=end.strftime(metadata_format_str),
        ) for s in PT_DET_HEADER]
        print(csvfname_detailed)
        print(metadata_detailed)
        write_report_to_csv(df, csvfname_detailed, metadata_detailed)
        pass

    #print(df)
    #print(start, end)

    # Init the dataframe for the output
    # TODO:  make code to populate this based on the results of the above loop.
    df_summary = pd.DataFrame({'OPERA Product Short Name': ['L3_DSWx_HLS'],
                               'Production Time (count)': [None],
                               #'Retrieval Time (P90)': [None],
                               'Production Time (min)': [None],
                               'Production Time (max)': [None],
                               'Production Time (mean)': [None],
                               'Production Time (median)': [None],
                               'Histogram': [None],
    })

    # TODO:  change the code below into a proper loop
    sub_df_in = df.loc[(df['OPERA Product Short Name'] == 'L3_DSWx_HLS')]
    #sub_df_p90 = calculate_P90(   sub_df_in, column).total_seconds()
    sub_df_min = calculate_min(   sub_df_in, column).total_seconds()
    sub_df_max = calculate_max(   sub_df_in, column).total_seconds()
    sub_df_avg = calculate_mean(  sub_df_in, column).total_seconds()
    sub_df_med = calculate_median(sub_df_in, column).total_seconds()

    print(sub_df_in.shape[0])
    ind_summary = (df_summary['OPERA Product Short Name'] == 'L3_DSWx_HLS')
    df_summary.loc[ind_summary, 'Production Time (count)'] = sub_df_in.shape[0]
    #df_summary.loc[ind_summary, 'Production Time (P90)'] = to_duration_isoformat(   sub_df_p90)
    df_summary.loc[ind_summary, 'Production Time (min)'] = to_duration_isoformat(   sub_df_min)
    df_summary.loc[ind_summary, 'Production Time (max)'] = to_duration_isoformat(   sub_df_max)
    df_summary.loc[ind_summary, 'Production Time (median)'] = to_duration_isoformat(sub_df_med)
    df_summary.loc[ind_summary, 'Production Time (mean)'] = to_duration_isoformat(  sub_df_avg)
    df_summary.loc[ind_summary, 'Histogram'] = generate_production_time_histogram(sub_df_in,
                                                                                  start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                                                  end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                                                                  'L3_DSWx_HLS',
                                                                                  column='Production Time',
                                                                                  outdir=outdir,
    )

    # This goes in the metadata header of the CSV
    csvfname_summary = osp.join(outdir, f'production-time-summary - {start.strftime(fname_format_str)} to {end.strftime(fname_format_str)}.csv')
    #metadata_summary = RT_SUM_HEADER.format(gen_datetime_utc=gen_datetime_utc,
    #                                        cov_start=start.strftime(metadata_format_str),
    #                                        cov_end=end.strftime(metadata_format_str),
    #)
    metadata_summary = [s.format(gen_datetime_utc=gen_datetime_utc,
                                 cov_start=start.strftime(metadata_format_str),
                                 cov_end=end.strftime(metadata_format_str),
    ) for s in PT_SUM_HEADER]

    print(csvfname_summary)
    print(metadata_summary)
    write_report_to_csv(df_summary, csvfname_summary, metadata_summary)
    #df_summary.to_csv(csvfname_summary)
    
    return df_summary



if __name__ == '__main__':
# Define command-line arguments
    parser = argparse.ArgumentParser(description='Compute statistics on the combined information in a list of CSV files.')
    parser.add_argument('report_type', metavar='REPORT', choices=['rt', 'pt'], help='Specify the report to generate')
    parser.add_argument('pattern', metavar='PATTERN', help='Filename pattern with wildcards for matching CSV files')
    parser.add_argument('--col', metavar='COLUMN', type=int, default=None, help='Column number to compute statistics on')
    parser.add_argument('--skip', metavar='LINES', type=int, default=0, help='Number of header lines to skip (default: 0)')
    parser.add_argument('--outdir', metavar='OUTDIR', type=str, default=None, help='Relative path to the directory for writing output files.  Defaults to being one directory up from the input files.')

    # Parse command-line arguments
    args = parser.parse_args()

    outdir = args.outdir
    if outdir is None:
        # Make this one directory up from PATTERN
        outdir = osp.dirname(osp.abspath(osp.dirname(args.pattern)))

    # Use glob on the provided pattern to generate the list of inputs
    input_files = glob.glob(args.pattern)

    if args.report_type == 'rt':
        if args.col is None:
            args.col = 'Retrieval Time'
        
        generate_retrieval_time_report(input_files, args.col, args.skip, outdir=outdir)

        
    if args.report_type == 'pt':
        if args.col is None:
            args.col = 'Production Time'

        generate_production_time_report(input_files, args.col, args.skip, outdir=outdir)

        

