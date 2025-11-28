import glob
import os

with open('headers.txt', 'w') as outfile:
    for f in glob.glob('*.csv'):
        with open(f, 'r') as infile:
            header = infile.readline().strip()
            outfile.write(f"{f}:{header}\n")
