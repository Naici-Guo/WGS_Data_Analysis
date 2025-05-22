'''
Markduplicates.
'''

# modules
from ruffus import *
import cgatcore.experiment as E
from cgatcore import pipeline as P
import cgatcore.iotools as iotools
import os
import sys
import glob

# mark duplicates
@transform("*.postalt.sorted.bam",
           suffix(".postalt.sorted.bam"),
           [".marked_duplicates.bam", ".marked_duplicates.metrics.txt"])
def mark_duplicates(input_file, output_files):
    output_bam, metrics_file = output_files
    threads = 4
    statement = ''' module load GATK/4.3.0.0-GCCcore-11.3.0-Java-11 &&
                     gatk MarkDuplicates
                       -I %(input_file)s
                       -O %(output_bam)s
                       -M %(metrics_file)s'''
    P.run(statement, job_memory="32G", job_threads=threads)

@follows(mark_duplicates)
def full():
    pass
def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)
if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
