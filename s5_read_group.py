"""
Add read group for each bam file and index again.
"""
# modules
from ruffus import *
import cgatcore.experiment as E
from cgatcore import pipeline as P
import cgatcore.iotools as iotools
import os
import sys
import glob

# add read group to marked duplicates bam files
@transform("*.marked_duplicates.bam",
           suffix(".marked_duplicates.bam"),
           ".rg.md.bam")
def add_read_group(input_file, output_file):
    sample_name = os.path.basename(input_file).replace('.marked_duplicates.bam', '')
    statement = ''' module load picard/2.25.1-Java-11 &&
                    java -jar $EBROOTPICARD/picard.jar AddOrReplaceReadGroups
                        I=%(input_file)s
                        O=%(output_file)s
                        RGID=unknown
                        RGLB=unknown
                        RGPL=DNBSEQ
                        RGPU=unknown
                        RGSM=%(sample_name)s'''
    P.run(statement, job_memory="4G")
# index bam file
@transform(add_read_group,
           suffix(".rg.md.bam"),
           ".rg.md.bam.bai")
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
