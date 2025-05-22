"""Post-processing step2 - sort & index."""
from ruffus import *
from cgatcore import pipeline as P
import glob
import os
import sys

# sort *.postalt.bam
@transform("*.postalt.bam",
           suffix(".postalt.bam"),
           ".postalt.sorted.bam")
def sort_post_alt_bam(input_file, output_file):
    threads = 4
    statement = '''samtools sort -@ 4 -m 4G -o %(output_file)s %(input_file)s'''
    P.run(statement, job_threads=threads)

# index bam file using samtools
@transform(sort_post_alt_bam,
           suffix('.postalt.sorted.bam'),
           '.postalt.sorted.bam.bai')
def index_bam(bamfile, bam_index):
    statement = '''module load SAMtools/1.16.1-GCC-11.3.0 &&
                   samtools index %(bamfile)s'''
    P.run(statement, job_memory="4G")

@follows(index_bam)
def full():
    pass
def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)
if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
