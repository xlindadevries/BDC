#!usr/bin/env python3


__author__ = "Linda de Vries"
__version__ = "1.0"


import multiprocessing as mp
from multiprocessing.managers import BaseManager
import sys, time, queue, argparse, csv

from fastq_functions import FastQC

POISONPILL = "MEMENTOMORI"
ERROR = "DOH"


def make_server_manager(host, port, authkey):
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

    manager = QueueManager(address=(host, port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager


def runserver(obj, chunks, host, port, outfile=None):
    """
    Run in server mode and send data
    """
    # Start a shared manager server and access its queues
    manager = make_server_manager(host, port, b'whathasitgotinitspocketsesss?')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not chunks:
        print("Nothing to do here!")
        return

    for chunk in chunks:
        shared_job_q.put({'fn': obj.read_file, 'args': (chunk)})

    print("Sending data!")
    time.sleep(2)

    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            if len(results) == len(chunks):
                print("All chunks have been processed and added to the results")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("There will no more data forthcoming...")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Server is finished")
    manager.shutdown()
    csv_writer(obj.calc_avg(results, True), outfile)


def make_client_manager(host, port, authkey):
    """
    Create a manager for a client. This manager connects to a server on the
    given address and exposes the get_job_q and get_result_q methods for
    accessing the shared queues from the server.
    Return a manager object.
    """

    class ServerQueueManager(BaseManager):
        """Server queue manager"""
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(host, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (host, port))
    return manager


def runclient(num_processes, host, port):
    """
    Starting a local client manager.
    """
    manager = make_client_manager(host, port, b'whathasitgotinitspocketsesss?')
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


def run_workers(job_q, result_q, num_processes):
    """
    Local client, the commandline core arguments of cores are turned into workers
    """
    processes = []
    for proc in range(num_processes):
        temp_proc = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temp_proc)
        temp_proc.start()
    print("Started %s workers!" % len(processes))
    for temp_proc in processes:
        temp_proc.join()


def peon(job_q, result_q):
    """
    Manage the jobs
    """
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print(f"This job died: {my_name}")
                return
            else:
                try:
                    result = job['fn'](job['args'])
                    print(f"Worker {my_name} busy with {job['args']}!")
                    result_q.put({'job': job, 'result': result})
                    print("Peon %s Workwork on %s!" % (my_name, job['args']))
                except NameError:
                    print("Worker cannot be found...")
                    result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            print(f"SLeeptime for: {my_name}")
            time.sleep(1)


def csv_writer(phred_dict, outfile=None):
    if outfile == None:
        outfile = "outfile.csv"
    else:
        outfile =  outfile
    with open(outfile, 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',')
        spamwriter.writerow(["base position", "phred score"])
        for key in phred_dict.keys():
            spamwriter.writerow([key, "{:.2f}".format(phred_dict[key])])
    print("The file '{}' is created.".format(outfile))


def main():
    # setup the command line arguments
    parser = argparse.ArgumentParser(description='Process some arguments')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "-s",
        action="store_true",
        help="Run the program in Server mode; see extra options needed below")
    mode.add_argument(
        "-c",
        action="store_true",
        help="Run the program in Client mode; see extra options needed below")
    server_args = parser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument(
        '-o', 
        '--outputFile',
        type=str,
        required=False,
        help='File name for the CSV file')
    server_args.add_argument(
        '--chunks',
        type=int,
        required=False,
        help='Number of chunks used to divide the workload')
    server_args.add_argument(
        'FastQFile',
        type=str,
        nargs='?',
        help='A FastQ file')
    client_args = parser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument(
        "-n",
        action="store",
        dest="n",
        required=False,
        type=int,
        help="Amount of cores to use as host")
    client_args.add_argument(
        "--host",
        action="store",
        type=str,
        required=True,
        help="The hostname where the Server is listening")
    client_args.add_argument(
        "--port",
        action="store",
        type=int,
        required=True,
        help="The port on which the Server is listening")
    args = parser.parse_args()

    # When the argument is given for server
    if args.s:
        print("Started Server side")
        fastqc_obj = FastQC(args.FastQFile, args.chunks)
        chunks = fastqc_obj.split_file()
        server = mp.Process(target=runserver,
                            args=(fastqc_obj, chunks, args.host, args.port, args.outputFile,))
        server.start()
        time.sleep(1)

    # When the argument is given for client
    if args.c:
        print("Started client side")
        client = mp.Process(target=runclient, args=(args.n, args.host, args.port,))
        client.start()
        client.join()


if __name__ == '__main__':
    sys.exit(main())