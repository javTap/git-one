#!/usr/bin/python

from optparse import OptionParser

from termcolor import cprint

"""
copy result from search "Chicago metro zip codes"

https://data.mongabay.com/igapo/zip_codes/metropolitan-areas/metro-alpha/Chicago%20(IL)1.html

on tmp.txt file, this script will output the list of all zip codes
"""


if __name__ == "__main__":

    parser = OptionParser( usage='usage: python %prog -i input_file', version="%prog 0.2")
    parser.add_option("-i", "--input_file", default='./tmp.txt', help='dump from web search')


    opts, _ = parser.parse_args()
    filename = opts.input_file
    outFile = "./parsed.txt"
    cprint(filename, 'red')

    zip_codes = []
    for line in file(filename):
        fld = line.split()
        zip_codes.append(fld[0])

    print list(set(zip_codes))


