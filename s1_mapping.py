"""
Pre-processing the raw sequence data to analysis-ready BAM files.
Step1: Map to reference
Start_file: 2*fastq files
Output_file: 
1. out.aln.bam: unsorted alignments with ALT-aware mapping quality.
2. out.hla.top: best genotypes for HLA-A, -B, -C, -DQA1, -DQB1 and -DRB1 genes.
3. out.hla.all: other possible genotypes on the six HLA genes.
4. out.log.*: bwa-mem, samblaster and HLA typing log files.
"""
from ruffus import *
from cgatcore import pipeline as P
import glob
import os
import sys

fastq_dir = "/mnt/parscratch/users/bi1ng/at_wgs/mapping_bwa/fastq_files"

r1_files = sorted(glob.glob(os.path.join(fastq_dir, "*.fastq.1.gz")))
starting_files = [(r1, r1.replace(".fastq.1.gz", ".fastq.2.gz")) for r1 in r1_files]

#print(f"Detected {len(starting_files)} paired FASTQ files.")

if len(starting_files) == 0:
    print("No input files found.")
    sys.exit(1)

@transform(
        starting_files,
        suffix(".fastq.1.gz"),
        [".aln.bam",
         ".hla.all",
         ".hla.top",
         ".log.bwamem",
         ".log.hla",
         ".log.samblaster"])
def map_dna_sequence(input_files, output_files):
    r1 = input_files[0]
    r2 = input_files[1]
    sample_name = os.path.basename(r1).replace(".fastq.1.gz", "")
    threads = 16
    statement = '''bwa.kit/run-bwamem -t %(threads)s -o %(sample_name)s -H reference/hs38DH.fa  %(r1)s %(r2)s | sh'''
    print(f"Running BWA mapping for {sample_name}")
    P.run(statement, job_memory="4G", job_threads=threads)

@follows(map_dna_sequence)
def full():
    pass

def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)

if __name__ == "__main__":
    sys.exit(P.main(sys.argv))