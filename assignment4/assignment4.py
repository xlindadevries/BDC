__author__ = "Linda de Vries"
__version__ = "1.0"

import argparse as ap
import sys
from statistics import mean


def control_fastq(fastq_file):
    """
    :param fastq_file: fastq file directory
    """
    all_line_lengths = []
    valid = True

    with open(fastq_file[0], "r") as file_obj:
        counter = 0
        total_counter = 0
        len_base_line = None
        len_phred_line = None

        # Check each base and phred line and check whether they're the same length
        for i, line in enumerate(file_obj):
            if valid:
                counter += 1
                total_counter += 1
                if counter == 2:
                    len_base_line = len(line.strip())
                    all_line_lengths.append(len_base_line)
                elif counter == 4:
                    len_phred_line = len(line.strip())
                    all_line_lengths.append(len_phred_line)
                    counter = 0

                    if not len_base_line == len_phred_line:
                        print(len_base_line, " ", len_phred_line)
                        valid = False
        i = i + 1

        if i % 4:
            valid = False

    # If its valid then it calculate the summary statistics
    if valid:
        all_max = str(max(all_line_lengths))
        all_min = str(min(all_line_lengths))
        mean_len = str(round(mean(all_line_lengths), 2))

        # Write to .csv on stdoutput
        file_name = sys.stdout.write(str(fastq_file[0]).split("/")[-1])
        sys.stdout.write(str(file_name) + "," + str(valid) + "," + all_min +
                         "," + all_max + "," + mean_len + "\n")
    else:
        file_name = sys.stdout.write(str(fastq_file[0]).split("/")[-1])
        sys.stdout.write(str(file_name) + "," + str(valid) + "\n")


def main():
    argparser = ap.ArgumentParser(description="Script voor Opdracht 2 van Big Data Computing;" \
                                              "Calculate PHRED scores over the network.")

    argparser.add_argument("fastq_files", action="store",
                           type=str, nargs='*',
                           help="Minstens 1 Illumina Fastq Format file om te verwerken")

    args = argparser.parse_args()

    control_fastq(args.fastq_files)


if __name__ == '__main__':
    main()
