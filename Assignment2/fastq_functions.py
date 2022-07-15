#!usr/bin/env python3

"""
fastq functions
"""

import os


class FastQC:

    def __init__(self, fastq_file, n_proc):
        self.fastq_file = fastq_file
        self.n_proc = n_proc


    def split_file(self):
        """
        Splits any file in bytes based on the amount of processes.
        """
        n_bytes = os.path.getsize(self.fastq_file)
        byte_chunks = ([n_bytes // self.n_proc + (1 if x < n_bytes % self.n_proc else 0)
        for x in range (self.n_proc)])
        positions = []
        start = 0
        end = byte_chunks[0]
        for chunk in byte_chunks:
            positions.append((start, end))
            start += chunk
            end += chunk
        return positions

    def read_file(self, file_parts):
        """
        Opens a FastQ file and calculates the PHRED score of each line that contains the quality value.
        """
        phred_dict = {}
        with open(self.fastq_file) as file_obj:
            file_obj.seek(file_parts[0])
            line = file_obj.readline()
            while line.startswith("+") == False and \
                    line.startswith("-") == False and file_obj.tell() < file_parts[1]:
                line = file_obj.readline()
            count = 3
            while line and file_obj.tell() < file_parts[1]:
                count += 1
                line = file_obj.readline()
                if count % 4 == 0:
                    for position, quality in enumerate(line.strip(), start=1):
                        if position in phred_dict:
                            phred_dict[position].append(ord(quality) - 33)
                        else:
                            phred_dict[position] = [ord(quality) - 33]
        return phred_dict

    def calc_avg(self, results, queue_dictionary):
        """
        calculates the avegrage score and stores it. 
        """
        combined_dict = {}
        for result_dictionary in results:
            if queue_dictionary == True:
                result_dictionary = result_dictionary['result']
            for key, value in result_dictionary.items():
                if key in combined_dict:
                    combined_dict[key] += result_dictionary[key]
                else:
                    combined_dict[key] = result_dictionary[key]
        return {key: round(sum(values) / len(values), 2)
                for key, values in result_dictionary.items()}

