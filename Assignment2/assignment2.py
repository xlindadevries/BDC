#!/usr/local/bin/python3

__author__ = "Linda de Vries"
__version__ = "1.0"

import argparse
import math
import multiprocessing as mp
import os
import queue
import time
import sys
from multiprocessing.managers import BaseManager

POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
AUTHKEY = b'whathasitgotinitspocketsesss?'


def make_server_manager(port, authkey):
    """ Create a manager for the server, listening on the given port.
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

    manager = QueueManager(address=('', port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager


def runserver(fn, data, IP, PORTNUM):
    # Start a shared manager server and access its queues
    manager = make_server_manager(PORTNUM, b'whathasitgotinitspocketsesss?')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not data:
        print("Gimme something to do here!")
        return

    files = data[0]
    chunks = data[1]
    output_file = data[2]

    # When there is data available, jobs are creating with a filename and their chunk of that file
    print("Sending the data...")

    for file in files:
        # get chunks per byte
        total_bytes = os.stat(file).st_size
        bytes_per_chunk = math.ceil(total_bytes / chunks)
        iterable = [x * bytes_per_chunk for x in range(chunks + 1)]

        file_chunks = [[iterable[x], iterable[x+1]] for x in range(len(iterable)-1)]
        queue_buckets = [[file, x] for x in file_chunks]

        for bucket in queue_buckets:
            shared_job_q.put({'fn': fn, 'arg': bucket})

        time.sleep(2)

        # Collecting the results
        results = []
        while True:
            try:
                result = shared_result_q.get_nowait()
                results.append(result)
                print(f"Got result {result} from result queue!")
                # If all chunks have been processed, exit the loop
                if len(results) == len(queue_buckets):
                    print("All chunks have been processed and added to the results")
                    break
            except queue.Empty:
                time.sleep(1)
                continue


        combined = combine_dicts([x['result'] for x in results])
        average = total_to_average(combined)
        write_to_csv(average, output_file, file)

    # Tell the client process no more data will be forthcoming
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Server is finished")
    manager.shutdown()


def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
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
def runclient(num_processes, IP, PORTNUM):
    """
    Starting a local client manager.
    """
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
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
    my_name = mp.current_process().name
    # the job loop
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
                    result = job['fn'](job['arg'])
                    print(f"Worker {my_name} busy with {job['arg']}!")
                    result_q.put({'job': job, 'result': result})
                    print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                # If the name is not valid, it will raise an error
                except NameError:
                    print("Worker cannot be found...")
                    result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            print(f"SLeeptime for: {my_name}")
            time.sleep(1)


def combine_dicts(dict_list):
    total_dict = {}
    for sub_dict in dict_list:
        for key in sub_dict:
            if key in total_dict:
                total_dict[key][0] += sub_dict[key][0]
                total_dict[key][1] += sub_dict[key][1]
            else:
                total_dict[key] = sub_dict[key]
    return total_dict


def write_to_csv(output, output_file, input_file):
    if output_file:
        input_file = input_file.split("/")[-1]
        name = input_file + "." + output_file
        file_obj = open(name, "w")
        for pos, score in enumerate(output):
            file_obj.write(f"{pos},{score}\n")
        file_obj.close()
    else:
        sys.stdout.write(f"{input_file}\n")
        for pos, score in enumerate(output):
            sys.stdout.write(f"{pos},{score}\n")


def total_to_average(base_dict, digits=3):
    avg_bases = []
    for key in base_dict.keys():
        avg_bases.append(round(base_dict[key][1]/base_dict[key][0]-33, digits))
    return avg_bases


def read_file(bucket):
    file = bucket[0]
    reading_frame = bucket[1]
    file_obj = open(file)
    file_obj.seek(reading_frame[0])

    line = file_obj.readline()

    while line != "+\n" and file_obj.tell() < reading_frame[1]:
        line = file_obj.readline()

    count = 3
    base_dict = {}
    while file_obj.tell() < reading_frame[1] and line:
        line = file_obj.readline()
        count += 1
        if count % 4 == 0:
            # in score line
            line = line.strip()
            for pos, c in enumerate(line):
                if pos in base_dict:
                    base_dict[pos][0] += 1
                    base_dict[pos][1] += ord(c)
                else:
                    base_dict[pos] = [1, ord(c)]
    file_obj.close()
    return base_dict


def argument_parser():
    """
    Argument parser for command line arguments.
    """
    # command line parameters
    parser = argparse.ArgumentParser(
        description="Script voor Opdracht 2 van Big Data Computing;  Calculate PHRED scores over the network.")
    mode = parser.add_mutually_exclusive_group(required=True)
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
    server_args = parser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument(
        "-o",
        action="store",
        dest="csvfile",
        type=argparse.FileType('w', encoding='UTF-8'),
        required=False,
        help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    server_args.add_argument(
        "fastq_files",
        action="store",
        type=argparse.FileType('r'),
        nargs='*',
        help="fastq file name, always first argument")
    server_args.add_argument(
        '--chunks',
        action="store",
        type=int,
        required=False,
        help='Amount of chunks the data nees to be split into')

    # Client sided arguments
    client_args = parser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument(
        '-n',
        '--number_of_cores',
        type=int,
        metavar='',
        required=False,
        help='Amount of cores the clients are allowed to use.')
    client_args.add_argument(
        "--host",
        action="store",
        type=str,
        default='',
        help="The hostname where the Server is listening")
    client_args.add_argument(
        "--port",
        action="store",
        type=int,
        default=5381,
        help="The port on which the Server is listening")

    args = parser.parse_args()
    main(args)


def main(args):
    server = args.server
    client = args.client

    # get filename argument
    if args.fastq_files:
        files = []
        for file in args.fastq_files:
            print(file)
            files.append(file.name)
        file = files[0]

    # get output file argument
    if args.csvfile:
        output_file = args.csvfile.name
    else:
        output_file = None

    # get the chunks argument
    chunks = args.chunks

    # get the cores argument
    cores = args.number_of_cores

    if server:
        print("started server")
        IP = args.host
        port = args.port

        data = [files, chunks, output_file]
        server = mp.Process(target=runserver, args=(read_file, data, IP, port))

        server.start()
        time.sleep(1)

    if client:
        print("Started client")
        IP = args.host
        port = args.port
        client = mp.Process(target=runclient, args=(cores, IP, port,))
        client.start()
        client.join()


if __name__ == "__main__":
    sys.exit(argument_parser())
