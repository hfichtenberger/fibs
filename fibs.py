import datetime
from genericpath import isfile
import os
import random
import signal
import socket
import string
import sys
import time

from argparse import ArgumentParser


__author__ = 'Hendrik'

fib_filename = '.fib'
fib_job_dir = 'jobs'
fib_log_dir = 'logs'
fib_preliminary_dir = 'preliminary'
fib_ready_dir = 'readystate'
fib_result_dir = 'results'

fib_scheduler_running_filename = '.fib_scheduler'
fib_scheduler_continuation_filename = 'continuation_file'

failed_suffix = '_failed'

class Job:
    def __init__(self):
        pass

    total_runs = 0
    remaining_runs = 0
    run_per_assignment = 1
    in_execution = 0
    command = ''
    fails = 0


def print_error(message):
    print("ERROR: " + message)


def print_warning(message):
    print("WARNING: " + message)


def log_message(filehandle, message):
    filehandle.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' ' + message + '\n')
    print(message)

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def delete_all_contents(directory):
    for currentdirname, dirnames, filenames in os.walk(directory, topdown=False):
        for filename in filenames:
            os.remove(os.path.join(currentdirname, filename))
        for dirname in dirnames:
            os.rmdir(os.path.join(currentdirname, dirname))


def cleanup_directory(clean_state_files=False):
    delete_all_contents(fib_job_dir)
    delete_all_contents(fib_preliminary_dir)
    if clean_state_files:
        delete_all_contents(fib_ready_dir)
        if os.path.isfile(fib_scheduler_running_filename):
            os.remove(fib_scheduler_running_filename)


def scheduler_start(args):
    # Boot scheduler
    if not os.path.isfile(fib_filename):
        print_error('Working directory was not initialized.')
        sys.exit(1)
    if os.path.isfile(fib_scheduler_running_filename) and not args.force:
        print_error('Scheduler is already running or has crashed. Use --force option to force using this ID.')
        sys.exit(1)
    open(fib_scheduler_running_filename, 'w').close()
    log_file = os.path.join(fib_log_dir, 'scheduler.txt')
    log_file = open(log_file, 'a')
    log_message(log_file, 'Scheduler started')

    # Register SIGTERM / SIGINT handler
    def signal_term_handler():
        if args.terminateworkers:
            cleanup_directory(True)
        else:
            cleanup_directory()
            os.remove(fib_scheduler_running_filename)
        sys.exit(0)
    signal.signal(signal.SIGABRT, signal_term_handler)
    signal.signal(signal.SIGINT, signal_term_handler)
    signal.signal(signal.SIGTERM, signal_term_handler)

    # Cleanup remainings from ungraceful terminations
    cleanup_directory()

    # Read configuration
    if args.jobfile.lower() == 'continue':
        if os.path.isfile(fib_scheduler_continuation_filename):
            job_file = open(fib_scheduler_continuation_filename, 'r')
        else:
            print_error('There exists no continuation file.')
            sys.exit(1)
    else:
        if os.path.isfile(fib_scheduler_continuation_filename):
            print_error('Please remove the continuation file "' + fib_scheduler_continuation_filename + '" to confirm '
                        'that you want to start over. Remember to possibly delete existing results and logs.')
            sys.exit(1)
        else:
            if not os.path.isfile(args.jobfile):
                print_error('Job file "' + args.jobfile + '" does not exist.')
                sys.exit(1)
            job_file = open(args.jobfile, 'r')
    active_jobs = list()
    finished_jobs = list()
    failed_jobs = list()
    for line in job_file:
        if not line.isspace():
            splitted = line.split(None, 2)
            new_job = Job()
            new_job.total_runs = int(splitted[0])
            new_job.remaining_runs = int(splitted[0])
            new_job.run_per_assignment = int(splitted[1])
            new_job.command = splitted[2].rstrip()
            active_jobs.append(new_job)
    job_file.close()

    # Scheduling
    idle_workers = set()
    busy_workers = dict()
    attempt_to_shutdown = False
    while True:
        # Check for new workers
        ready_files = [filename for filename in os.listdir(fib_ready_dir) if isfile(os.path.join(fib_ready_dir, filename))]
        for filename in ready_files:
            if not (filename in idle_workers or filename in busy_workers):
                log_message(log_file, 'New worker: ' + filename)
                prel_dirname = os.path.join(fib_preliminary_dir, filename)
                if not os.path.isdir(prel_dirname):
                    os.mkdir(prel_dirname)
                idle_workers.add(filename)
        # Check for busy -> idle workers
        for worker in busy_workers.copy():
            if not os.path.isfile(os.path.join(fib_job_dir, worker)):
                # Move results
                outdir = os.path.join(fib_preliminary_dir, worker)
                job = busy_workers[worker][0]
                job.in_execution -= 1
                failed_filepath = os.path.join(fib_job_dir, worker + failed_suffix)
                if not os.path.isfile(failed_filepath):
                    # Job successful
                    log_message(log_file, 'Idle worker: Job completed on ' + worker)
                    for filename in os.listdir(outdir):
                        from_fullname = os.path.join(outdir, filename)
                        if os.path.isfile(from_fullname):
                            to_fullname = os.path.join(fib_result_dir, filename)
                            if os.path.isfile(to_fullname):
                                to_fullname += '_' + id_generator()
                            os.rename(from_fullname, to_fullname)
                    if job.remaining_runs == 0 and job.in_execution == 0:
                        active_jobs.remove(job)
                        finished_jobs.append(job)
                else:
                    # Job failed
                    log_message(log_file, 'Idle worker: Job failed on ' + worker)
                    job.remaining_runs += busy_workers[worker][1]
                    job.fails += 1
                    os.remove(failed_filepath)
                    if job.fails >= args.resign:
                        active_jobs.remove(job)
                        failed_jobs.append(job)
                        log_message(log_file, 'Removed job from schedule due to too many failed runs.')
                # Manage worker pool
                busy_workers.pop(worker)
                idle_workers.add(worker)
        # Check for idle -> terminated workers
        for worker in idle_workers.copy():
            if not os.path.isfile(os.path.join(fib_ready_dir, worker)):
                log_message(log_file, 'Terminated worker: ' + worker)
                idle_workers.remove(worker)
        # Schedule jobs
        while not attempt_to_shutdown and len(idle_workers) > 0:
            # Get next job
            try:
                job = next(job for job in active_jobs if job.remaining_runs > 0)
            except StopIteration:
                attempt_to_shutdown = True
                break
            # Get worker and assign job
            worker = idle_workers.pop()
            num_assigned_jobs = min(job.run_per_assignment, job.remaining_runs)
            busy_workers[worker] = (job, num_assigned_jobs)
            job.remaining_runs -= num_assigned_jobs
            job.in_execution += 1
            # Prepare preliminary output directory
            outdir = os.path.join(fib_preliminary_dir, worker)
            if os.path.isdir(outdir):
                delete_all_contents(outdir)
            else:
                os.mkdir(outdir)
            # Prepare job file
            prel_jobfilename = os.path.join(fib_job_dir,  id_generator())
            real_jobfilename = os.path.join(fib_job_dir, worker)
            prel_jobfile = open(prel_jobfilename, 'w')
            prel_jobfile.write(str(num_assigned_jobs) + ' ' + job.command + '\n')
            prel_jobfile.close()
            # Go worker!
            os.rename(prel_jobfilename, real_jobfilename)
            log_message(log_file, 'Busy worker: Scheduled on worker ' + worker + " the job " + job.command)
        # Ready-state file removed?
        if not os.path.isfile(fib_scheduler_running_filename) and not attempt_to_shutdown:
            log_message(log_file, 'Ready-state file was delete. Scheduler will stop after all workers completed.')
            attempt_to_shutdown = True
        # No busy workers means we are done
        if attempt_to_shutdown and len(busy_workers) == 0:
            break
        # Idle-Sleep
        time.sleep(args.idletime)

    # Write remaining jobs into file
    remaining_jobs_file = open(fib_scheduler_continuation_filename, 'w')
    wrote_continuation_file = False
    for job in active_jobs + failed_jobs:
        if job.remaining_runs > 0:
            wrote_continuation_file = True
            remaining_jobs_file.write(str(job.remaining_runs) + ' ' + str(job.run_per_assignment) + ' ' + str(job.command) + '\n')
    remaining_jobs_file.close()
    if wrote_continuation_file:
        log_message(log_file, 'Wrote continuation file because there are remaining jobs.')
    else:
        os.remove(fib_scheduler_continuation_filename)

    log_message(log_file, 'Scheduler stopped')
    signal_term_handler()


def worker_start(args):
    # Boot worker
    if not os.path.isfile(fib_filename):
        print_error('Working directory was not initialized.')
        sys.exit(1)
    identifier = socket.gethostname() + "$" + args.identifier
    ready_filename = os.path.join(fib_ready_dir, identifier)
    if os.path.isfile(ready_filename) and not args.force:
        print_error('A worker with the ID ' + args.identifier + ' is already in running or has crashed. '
                                                                'Use --force option to force using this ID.')
        sys.exit(1)
    open(ready_filename, 'w').close()
    job_filename = os.path.join(fib_job_dir, identifier)
    log_file = os.path.join(fib_log_dir, identifier + '.txt')
    log_file = open(log_file, 'a')
    log_message(log_file, 'Worker ' + identifier + ' started')

    # Register SIGTERM / SIGINT handler
    def signal_term_handler():
        if os.path.isfile(ready_filename):
            os.remove(ready_filename)
        sys.exit(0)
    signal.signal(signal.SIGABRT, signal_term_handler)
    signal.signal(signal.SIGINT, signal_term_handler)
    signal.signal(signal.SIGTERM, signal_term_handler)

    output_dir = os.path.join(fib_preliminary_dir, identifier)
    while True:
        if not os.path.isfile(ready_filename):
            log_message(log_file, 'Ready-state file was deleted. Worker will stop.')
            break
        if not os.path.isfile(job_filename):
            time.sleep(args.idletime)
        else:
            job_file = open(job_filename, 'r')
            abort = False
            for line in job_file:
                if not line.isspace():
                    entry = line.split(None, 1)
                    entry[1] = entry[1].replace('$HOST$', socket.gethostname())
                    entry[1] = entry[1].replace('$INSTANCE$', args.identifier)
                    entry[1] = entry[1].replace('$OUTPUT$', output_dir)
                    entry[1] = entry[1].rstrip()
                    for i in range(1, int(entry[0])+1, 1):
                        log_message(log_file, 'Executing job (' + str(i) + '/' + entry[0] + '): ' + entry[1])
                        retval = os.system(entry[1])
                        log_message(log_file, 'Finished job. Return value: ' + str(retval))
                        if retval != 0:
                            log_message(log_file, 'Job failed. Aborting whole job batch.')
                            open(os.path.join(fib_job_dir, identifier + failed_suffix, 'w').close())
                            delete_all_contents(output_dir)
                            abort = True
                            break
                    if abort:
                        break
            job_file.close()
            os.remove(job_filename)

    log_message(log_file, 'Worker ' + identifier + ' stopped')
    log_file.close()
    signal_term_handler()


def initialize(args):
    if os.path.isfile(fib_filename) and not args.cleanup:
        print_warning('Working directory was already initialized. '
                      'Use --cleanup option to remove temporary files (not including results, logs and ready-state '
                      'files). Before using this option be sure that no scheduler and no worker is running!')
    open(fib_filename, 'w').close()
    if args.cleanup:
        cleanup_directory(True)
    if not os.path.isdir(fib_log_dir):
        os.mkdir(fib_log_dir)
    if not os.path.isdir(fib_job_dir):
        os.mkdir(fib_job_dir)
    if not os.path.isdir(fib_preliminary_dir):
        os.mkdir(fib_preliminary_dir)
    if not os.path.isdir(fib_ready_dir):
        os.mkdir(fib_ready_dir)
    if not os.path.isdir(fib_result_dir):
        os.mkdir(fib_result_dir)


if __name__ == '__main__':
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_scheduler = subparsers.add_parser('scheduler')
    assert isinstance(parser_scheduler, ArgumentParser)
    subparsers_scheduler = parser_scheduler.add_subparsers()
    parser_scheduler_start = subparsers_scheduler.add_parser('start')
    assert isinstance(parser_scheduler_start, ArgumentParser)
    parser_scheduler_start.set_defaults(func=scheduler_start)
    parser_scheduler_start.add_argument('jobfile', help='Path to job file or "continue"')
    parser_scheduler_start.add_argument('-f', '--force', action='store_true',
                                        help='Scheduler will start even if the existence of "' +
                                             fib_scheduler_running_filename + '" file indicates that the worker is'
                                             'already running.')
    parser_scheduler_start.add_argument('-i', '--idletime', action='store', type=float, default=5,
                                        help='Timespan to sleep when there is nothing to to (polling interval).')
    parser_scheduler_start.add_argument('-r', '--resign', action='store', type=int, default=5,
                                        help='Number of times a job is allowed to fail before it is removed from'
                                             'scheduling.')
    parser_scheduler_start.add_argument('-w', '--terminateworkers', action='store_true',
                                        help='Scheduler will send termination signals to all workers after'
                                              'completing job list.')

    parser_worker = subparsers.add_parser('worker')
    assert isinstance(parser_worker, ArgumentParser)
    subparsers_worker = parser_worker.add_subparsers()
    parser_worker_start = subparsers_worker.add_parser('start')
    assert isinstance(parser_worker_start, ArgumentParser)
    parser_worker_start.add_argument('identifier', type=str, default='1',
                                     help='Unique identifier on this machine')
    parser_worker_start.add_argument('-f', '--force', action='store_true',
                                     help='Worker will start even if the existence of a ready-state file in' +
                                          fib_ready_dir + 'indicates that the worker is already running.')
    parser_worker_start.add_argument('-i', '--idletime', action='store', type=float, default=5,
                                     help='Timespan to sleep when there is nothing to to (polling interval).')
    parser_worker.set_defaults(func=worker_start)

    parser_init = subparsers.add_parser('init')
    assert isinstance(parser_init, ArgumentParser)
    parser_init.set_defaults(func=initialize)
    parser_init.add_argument('-c', '--cleanup', action='store_true', help='Remove temporary files.'
                                                                          'Does not include results, logs and'
                                                                          'ready-state files.')

    args = parser.parse_args()
    args.func(args)