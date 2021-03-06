"""
Merge files that follow a pattern and merged them in a single file with county information taken
from the  filename
"""

import fnmatch
import os
from optparse import OptionParser

import pandas as pd

DEBUG=True

if __name__ == "__main__":

    parser = OptionParser( usage='usage: python %prog -i input_file -p postFix_pattern  -s state', version="%prog 0.2")

    parser.add_option("-i", "--input_dir", default='../dataFormatting/allData/', help='directory path')
    parser.add_option("-p", "--post_fix", default='_merged.csv', help="file's post fix pattern")

    opts, _ = parser.parse_args()

    indata = opts.input_dir
    matchPattern = '*.csv'
    outFile = ''
    if os.path.isdir(indata):
        outFile = indata + os.sep + 'Merged' + opts.post_fix
        print outFile

    # Use os and fnmatch to list all files matching matchPattern of the file type CSV in the current directory
    csv_files = fnmatch.filter(os.listdir(indata), matchPattern)
    # print csv_files

    dfs = []

    for csv_file in csv_files:
        """
        we don't want to parse every column in the csv file. To only read certain columns we can use the parameter usecols.
        """
        df1 = pd.read_csv(indata + os.sep + csv_file)

        df2 = df1[df1['calc_acreage']>35]
        try:
            df2.drop(['land_use_class'], axis=1, inplace=True)
        except:
            pass
        dfs.append((df2))


    if len(dfs):
        # use the method concat to concatenate the dataframes
        aggregatedDf = pd.concat(dfs, sort=False)

        aggregatedDf.to_csv(outFile, index=False)

    else:
        print "Empty"


