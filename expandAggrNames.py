import fnmatch
import os
import re
from optparse import OptionParser

import numpy as np
import pandas as pd
from termcolor import cprint

# this is a change to test git

"""
Expand names from aggregated colunm
i.e.: from IA_Guthrie_County_modified_wDist_filtered.csv:

                                                   index                            name_alias mail_address_alias mailing_city mailing_state clean_zip calc_acreage
HARPER KREG HARPER DOYLE DEED ( 229.82),STEVENS...    10  STEVENS FAMILY FARM LLC DEED et. al.        2202 4TH ST       ELDORA            IA     50627       522.99


12       229.82     50627         2202 4TH ST       ELDORA            IA                     HARPER KREG HARPER DOYLE DEED   Guthrie    IA
13       293.17     50627         2202 4TH ST       ELDORA            IA                      STEVENS FAMILY FARM LLC DEED   Guthrie    IA
"""

# DEBUG = True
DEBUG = False
PARTIAL_DEBUG=True      # just to modify master branch

if __name__ == "__main__":

    parser = OptionParser( usage='usage: python %prog -i input_file -p postFix_pattern  -s state', version="%prog 0.2")

    # parser.add_option("-i", "--input_dir", default='../dataFormatting/IOWA/PROCESSED_FILES/_UPDATED_W_DISTANCE_/_OUTPUT_FILTERED_TEST/IA_Guthrie_County_modified_wDist_filtered.csv', help='directory path')
    parser.add_option("-i", "--input_dir", default='../dataFormatting/IOWA/PROCESSED_FILES/_UPDATED_W_DISTANCE_/_OUTPUT_FILTERED_TEST/', help='directory path')
    # parser.add_option("-i", "--input_dir", default='../dataFormatting/IOWA/PROCESSED_FILES/_UPDATED_W_DISTANCE_/_OUTPUT_FILTERED/', help='directory path')
    # parser.add_option("-i", "--input_dir", default='./', help='directory path')
    parser.add_option("-p", "--post_fix", default='_filtered.csv', help="file's post fix pattern")
    # parser.add_option("-p", "--post_fix", default='_intermediate.csv', help="file's post fix pattern")
    parser.add_option("-s", "--state", default='IA', help="state")

    opts, _ = parser.parse_args()
    indata  = opts.input_dir

    indataIsADir = False
    if os.path.isdir(indata):
        dirFiles = os.listdir(indata)
        allFiles = [indata+x for x in dirFiles]
        indataIsADir = True

        inDir =  indata
        print "is a directory !!!! ", allFiles
    else:

        allFiles = [indata]
        dirFiles = [os.path.basename(indata)]
        indata =  os.path.dirname(indata)

    if DEBUG:
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 200)

    state = opts.state
    matchPattern = state + '_*_County*' +  opts.post_fix
    # print "matchPattern ", matchPattern
    outFile = ''
    if os.path.isdir(indata):
        outFile = indata + os.sep + state + opts.post_fix
        # print outFile

    # Use os and fnmatch to list all files matching matchPattern of the file type CSV in the current directory
    # print 'allFiles  ', allFiles
    csv_files = fnmatch.filter(dirFiles, matchPattern)

    # print csv_files

    # list to collect all loaded CSV files into dataframes
    # dfs = [pd.read_csv('.' + os.sep + csv_file) for csv_file in csv_files]
    dfs = []

    matchNamePattern = r"^%s_(\w+)_County" % ( state)
    for csv_file in csv_files:

        cprint("csv_file %s "% csv_file, 'red')
        """
        we don't want to parse every column in the csv file. To only read certain columns we can use the parameter usecols.
        """
        df1 = pd.read_csv(indata + os.sep + csv_file)
        """
        "w+" will match the words between patterns 'IA_' and '_County' and thereafter, anything else is not identified
        """
        r1 = re.match(matchNamePattern, csv_file)

        county= r1.groups()[0]

        # find all not-null rows indexes on the 'aggregated_Names' columm
        withAggregate = np.where(df1['aggregated_Names'].notnull())[0]

        newDf = df1.loc[withAggregate]

        # cprint("County %s " % county, 'red')
        # cprint (newDf, 'blue')


        aggregateList = newDf.aggregated_Names.values

        newDf.reset_index(inplace=True)
        expandAggr = pd.DataFrame(columns=newDf.columns.values)
        newDf.set_index('aggregated_Names', inplace=True)
        for names in aggregateList:
            rowName = newDf.loc[names,:].to_frame().T
            # cprint ( rowName, 'cyan')
            mamesPerEntry = names.split(',')
            numLines = len(mamesPerEntry)

            for i in mamesPerEntry:
                name,acres = i.split('(')
                rowName.name_alias = name
                rowName.calc_acreage = acres.replace(')','')
                expandAggr = pd.concat([expandAggr, rowName], ignore_index=True,sort=True)


        # cprint(expandAggr, 'green')

        expandAggr.drop(columns=['index'], inplace=True)

        expandAggr['County'] = pd.Series([county] * len(expandAggr))
        expandAggr['State'] = pd.Series([state] * len(expandAggr))
        expandAggr['name_alias'] = expandAggr['name_alias'].str.replace('DEED', '')

        # """
        # remove redundant names, concatenate 'new' expanded dayaframe with read dataframe
        # """
        df1.drop(withAggregate, inplace=True)

        df1['County'] = county
        df1['State'] = state
        df1['name_alias'] = df1['name_alias'].str.replace('DEED', '')

        df1 =  pd.concat([df1, expandAggr], ignore_index=True, sort=True)

        dfs.append((df1))


    if len(dfs):
        # use the method concat to concatenate the dataframes
        aggregatedDf = pd.concat(dfs, sort=False, ignore_index=True)
        aggregatedDf.drop(columns=['aggregated_Names'], inplace=True)
        if DEBUG:

            print aggregatedDf
        else:
            aggregatedDf.to_csv(outFile, index=False)

    else:
        print "Empty"
