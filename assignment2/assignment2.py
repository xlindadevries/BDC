#!/usr/local/bin/python3.7

__author__ = "Linda de Vries"
__version__ = "1.0"

import multiprocessing as mp
from multiprocessing.managers import BaseManager
import csv
import os
import queue
import sys
import time
import math
import argparse as ap

POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
AUTHKEY = b'whathasitgotinitspocketsesss?'


def make_server_manager(ip, port):
    """
    Create a manager for the server, listening on the given port.
    Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=(ip, port), authkey=AUTHKEY)
    manager.start()
    print('Server started at port %s' % port)
    return manager


def runserver(fn, data, ip, port, outfile):
    # Start a shared manager server and access its queues
    manager = make_server_manager(ip, port)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not data[0]:
        print("Gimme something to do here!")
        return

    file = data[0]
    chunks = data[1]

    # When there is data available, jobs are creating with a filename and their chunk of that file
    print("Sending the data...")
    for i in range(len(chunks)):
        if not i + 1 == len(chunks):
            shared_job_q.put({'fn': fn, 'arg': (file, chunks[i], chunks[i + 1])})

    time.sleep(2)

    # After putting all the data in the Queue, the POISONPILL is added
    shared_job_q.put(POISONPILL)

    # Sleeping for 2 seconds
    time.sleep(2)

    # Collecting the results
    results = []
    while True:
        # Trying to get the results from the results queue
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print(f"Got result {result} from result queue!")

            # If all chunks have been processed, exit the loop
            if len(results) == len(chunks) - 1:
                print("All chunks have been processed and added to the results")
                break

        # If no results are found in the queue, wait a moment before trying again
        except queue.Empty:
            time.sleep(1)
            continue

    # Sleep for a moment to give the clients time to die
    print("Server is finished")
    manager.shutdown()

    # Write the results to output
    avg_phreds = process_mp_outputs(results)
    print(results)
    write_csv(outfile, avg_phreds)


def make_client_manager(ip, port, authkey):
    """
    Create a manager for a client. This manager connects to a server on the
    given address and exposes the get_job_q and get_result_q methods for
    accessing the shared queues from the server.
    Return a manager object.
    """

    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager


# Client runner
def runclient(num_processes, ip, port):
    """
    Starting a local client manager.
    """
    manager = make_client_manager(ip, port, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


# The client worker
def run_workers(job_q, result_q, num_processes):
    """
    Local client, the commandline core arguments of cores are turned into workers
    """
    processes = []
    # For each process, create a worker
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()


# Worker
def peon(job_q, result_q):
    """
    Worker function executing a job and appending it to the result queue
    """
    # Stores the name of job
    my_name = mp.current_process().name

    # The job loop
    while True:
        # If the poisonpill comes, all tasks are finished and the worker is exterminated
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print(f"This job died: {my_name}")
                return
            # Else execute the job and append to result queue
            else:
                try:
                    result = job['fn'](*job['arg'])
                    print(f"Worker {my_name} busy with {job['arg']}!")
                    result_q.put({'job': job, 'result': result})
                    print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                # If the name is not valid, it will raise an error
                except NameError:
                    print("Worker cannot be found...")
                    result_q.put({'job': job, 'result': ERROR})
        # If there are no jobs, but also no death, just take some rest
        except queue.Empty:
            print(f"SLeeptime for: {my_name}")
            time.sleep(1)


def get_avg_phreds(fastq, start_byte, end_byte):
    """
    Opens the file, and gets the average phred score
    """
    # Finding the first complete header and setting that as new start
    with open(fastq[0], "r") as fastq_file:
        # If the start byte is not 0, find the nearest upcoming header
        fastq_file.seek(start_byte)
        if not start_byte == 0:
            start_bytes = []
            for line in fastq_file:
                start_bytes.append(fastq_file.tell())
                if line.decode("utf-8").startswith("@DE18PCC"):
                    break
            fastq_file.seek(start_bytes[-2])
        # Loop over all the lines to find the quality strings
        phreds_dict = {}
        for count, line in enumerate(fastq_file, start=1):
            if line.decode("utf-8").startswith("@DE18PCC"):
                if fastq_file.tell() > end_byte:
                    break
            # When the quality string has been found, process it
            if count % 4 == 0:
                qualities = list(line)[:-1]
                for base_nr, quality in enumerate(qualities, start=1):
                    if base_nr not in phreds_dict:
                        phreds_dict[base_nr] = [quality]
                    else:
                        phreds_dict[base_nr].append(quality)

    # Returning the avg phred for position as lists in a list
    return [[key, round(sum(phreds_dict[key]) / len(phreds_dict[key]))] for key in phreds_dict]


def process_mp_outputs(results):
    """
    Parsing the different output lists, and collecting the different averages in a dict
    """
    avg_phreds = {}
    # Iterating over each result
    for result in results:
        result = result['result']
        # Appending the average for that base to the correct list in the dict
        for phred in result:
            if phred[0] not in avg_phreds:
                avg_phreds[phred[0]] = [phred[1]]
            else:
                avg_phreds[phred[0]].append(phred[1])

    # Returning the average phred score per base position for ALL qualities this time
    return [[key, (sum(avg_phreds[key]) / len(avg_phreds[key])) - 33] for key in avg_phreds]


def write_csv(outfile, avg_phreds):
    """
    Writes the base number and average phred score for tht position to the csv
    """
    if outfile:
        with open(outfile, "w") as out:
            for pos, score in enumerate(outfile):
                out.write(f"{pos}, {score}\n")
    else:
        for pos, score in enumerate(avg_phreds):
            sys.stdout.write(f"{pos},{score}\n")


def main():
    # command line parameters
    argparser = ap.ArgumentParser(
        description="Script voor Opdracht 2 van Big Data Computing;  Calculate PHRED scores over the network.")
    mode = argparser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "-s",
        "--server",
        action="store_true",
        help="Run the program in Server mode; see extra options needed below")
    mode.add_argument(
        "-c",
        "--client",
        action="store_true",
        help="Run the program in Client mode; see extra options needed below")

    # Server sided arguments
    server_args = argparser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument(
        "-o",
        action="store",
        dest="csvfile",
        type=ap.FileType('w', encoding='UTF-8'),
        required=False,
        help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    server_args.add_argument(
        "fastq_files",
        action="store",
        type=ap.FileType('r'),
        nargs='*',
        help="Minstens 1 Illumina Fastq Format file om te verwerken")
    server_args.add_argument(
        "-chunks",
        "--num_of_chunks",
        action="store",
        type=int,
        required=True)

    # Client sided arguments
    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument(
        "-n",
        "--num_of_cores",
        action="store",
        required=False,
        type=int,
        help="amount of cores for using per host.")
    client_args.add_argument(
        "--host",
        action="store",
        type=str,
        help="The hostname where the Server is listening")
    client_args.add_argument(
        "--port",
        action="store",
        type=int,
        help="The port on which the Server is listening")

    # Store all the arguments
    args = argparser.parse_args()

    # Make the server, client and other variables
    server = args.server
    client = args.client

    # Get the filename
    if args.fastq_files:
        files = []
        for file in args.fastq_files:
            files.append(file.name)
        filename = files[0]

    # Get the output
    outfile = args.csvfile

    # Starting the server when -s is given in the command line
    if server:
        ip = ""
        port = args.port
        # Calculating the start and stop values
        total_bytes = os.path.getsize(filename)
        bytes_per_chunk = math.ceil(total_bytes / args.num_of_chunks)

        chunks = [i for i in range(0, total_bytes, bytes_per_chunk)]
        chunks.append(total_bytes)

        # Store file name and bites in a variable
        data = [filename, chunks]
        server = mp.Process(target=runserver, args=(get_avg_phreds, data, ip, port, outfile))
        server.start()
        time.sleep(1)

    # Starting the client when -c is given in the command line
    if client:
        port = args.port
        ip = args.host
        client = mp.Process(target=runclient, args=(args.num_of_cores, ip, port,))
        client.start()
        client.join()


if __name__ == "__main__":
    sys.exit(main())
