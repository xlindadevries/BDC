#!/bin/bash

echo "Filenaam,Valide,Min_length,Max_length,Average_length"
find /data/dataprocessing/rnaseq_data/Brazil_Brain/*.fastq -type f | parallel -j2 --sshloginfile parallel_ssh.txt --results gnu_res "python3 assignment4.py {}"
