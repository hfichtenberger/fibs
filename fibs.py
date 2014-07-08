__author__ = 'Hendrik'

import __builtin__


def tryimport(name, globals={}, locals={}, fromlist=[], level=-1):
    try:
        return realimport(name, globals, locals, fromlist, level)
    except ImportError:
        if name == 'argparse' and sys.version_info[0] <= 2 and sys.version_info[1] <= 6:
            print('Python <= 2.6: Please download argparse.py from argparse package and place it in the same'
                  'directory as this file.')
            sys.exit(1)
        else:
            raise


realimport, __builtin__.__import__ = __builtin__.__import__, tryimport

import datetime
import os
import random
import signal
import socket
import string
import sys
import time

from argparse import ArgumentParser


fib_filename = '.fib'
fib_job_dir = 'jobs'
fib_log_dir = 'logs'
fib_temp_dir = 'tmp'
fib_token_dir = 'tokens'
fib_result_dir = 'results'

fib_scheduler_running_filename = 'scheduler_running'
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


def log_message(filehandle, message):
    filehandle.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' ' + message + '\n')
    print(message)


def get_input(lowercase=False):
    answer = raw_input().strip()
    answer = answer.lower() if lowercase else answer
    return answer


def assert_working_dir():
    if not os.path.isfile(fib_filename):
        print_error('Working directory was not initialized.')
        sys.exit(1)


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
    delete_all_contents(fib_temp_dir)
    if clean_state_files:
        delete_all_contents(fib_token_dir)
        if os.path.isfile(fib_scheduler_running_filename):
            os.remove(fib_scheduler_running_filename)


def scheduler_start(args):
    # Boot scheduler
    assert_working_dir()
    if os.path.isfile(fib_scheduler_running_filename):
        print_error('Scheduler is already running or has crashed. In case of the latter, consider the repair option.')
        sys.exit(1)
    open(fib_scheduler_running_filename, 'w').close()
    log_file = os.path.join(fib_log_dir, 'scheduler.txt')
    log_file = open(log_file, 'a')
    log_message(log_file, 'Scheduler started')

    # Register SIGTERM / SIGINT handler
    def exit_gracefully(exitcode=0):
        cleanup_directory()
        if os.path.isfile(fib_scheduler_running_filename):
            os.remove(fib_scheduler_running_filename)
        sys.exit(exitcode)

    signal.signal(signal.SIGABRT, exit_gracefully)
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    # Cleanup remainings from ungraceful terminations
    cleanup_directory()

    # Get configuration
    if args.subparser_name == 'start':
        if os.path.isfile(fib_scheduler_continuation_filename):
            print_error('Please remove the continuation file "' + fib_scheduler_continuation_filename + '" to confirm '
                        'that you want to start over. You might also want to delete existing results and logs by '
                        'using the reset command.')
            exit_gracefully(1)
        else:
            if not os.path.isfile(args.jobfile):
                print_error('Job file "' + args.jobfile + '" does not exist.')
                exit_gracefully(1)
            job_file = open(args.jobfile, 'r')
    else:
        if os.path.isfile(fib_scheduler_continuation_filename):
            job_file = open(fib_scheduler_continuation_filename, 'r')
        else:
            print_error('No continuation file.')
            exit_gracefully(1)
    # Read configuration
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
        ready_files = [filename for filename in os.listdir(fib_token_dir) if
                       os.path.isfile(os.path.join(fib_token_dir, filename))]
        for filename in ready_files:
            if not (filename in idle_workers or filename in busy_workers):
                log_message(log_file, 'New worker: ' + filename)
                prel_dirname = os.path.join(fib_temp_dir, filename)
                if not os.path.isdir(prel_dirname):
                    os.mkdir(prel_dirname)
                idle_workers.add(filename)
        # Check for busy -> idle workers
        for worker in busy_workers.copy():
            if not os.path.isfile(os.path.join(fib_job_dir, worker)):
                # Move results
                outdir = os.path.join(fib_temp_dir, worker)
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
            if not os.path.isfile(os.path.join(fib_token_dir, worker)):
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
            outdir = os.path.join(fib_temp_dir, worker)
            if os.path.isdir(outdir):
                delete_all_contents(outdir)
            else:
                os.mkdir(outdir)
            # Prepare job file
            prel_jobfilename = os.path.join(fib_job_dir, id_generator())
            real_jobfilename = os.path.join(fib_job_dir, worker)
            prel_jobfile = open(prel_jobfilename, 'w')
            prel_jobfile.write(str(num_assigned_jobs) + ' ' + job.command + '\n')
            prel_jobfile.close()
            # Go worker!
            os.rename(prel_jobfilename, real_jobfilename)
            log_message(log_file, 'Busy worker: Scheduled on worker ' + worker + " the job " + job.command)
        # Token file removed?
        if not os.path.isfile(fib_scheduler_running_filename) and not attempt_to_shutdown:
            log_message(log_file, 'Token file was delete. Scheduler will stop after all workers completed.')
            attempt_to_shutdown = True
        # No busy workers means we are done
        if attempt_to_shutdown and len(busy_workers) == 0:
            break
        # Flush logfile
        log_file.flush()
        # Idle-Sleep
        time.sleep(args.idletime)

    # Write remaining jobs into file
    remaining_jobs_file = open(fib_scheduler_continuation_filename, 'w')
    wrote_continuation_file = False
    for job in active_jobs + failed_jobs:
        if job.remaining_runs > 0:
            wrote_continuation_file = True
            remaining_jobs_file.write(
                str(job.remaining_runs) + ' ' + str(job.run_per_assignment) + ' ' + str(job.command) + '\n')
    remaining_jobs_file.close()
    if wrote_continuation_file:
        log_message(log_file, 'Wrote continuation file because there are remaining jobs.')
    else:
        os.remove(fib_scheduler_continuation_filename)

    # Exit workers?
    if args.exitworker:
        delete_all_contents(fib_token_dir)

    log_message(log_file, 'Scheduler stopped')
    exit_gracefully()


def scheduler_stop(args):
    if not os.path.isfile(fib_scheduler_running_filename):
        print_error('No scheduler running in this working directory.')
        sys.exit(1)
    else:
        os.remove(fib_scheduler_running_filename)
        print('The scheduler will gracefully exit after all running workers finished their current jobs.')


def worker_start(args):
    # Boot worker
    assert_working_dir()
    identifier = socket.gethostname() + "$" + args.identifier
    ready_filename = os.path.join(fib_token_dir, identifier)
    if os.path.isfile(ready_filename):
        print_error('A worker with the ID ' + args.identifier + ' is already in running or has crashed.'
                    'In case of the latter, consider the repair option.')
        sys.exit(1)
    open(ready_filename, 'w').close()
    job_filename = os.path.join(fib_job_dir, identifier)
    log_file = os.path.join(fib_log_dir, identifier + '.txt')
    log_file = open(log_file, 'a')
    log_message(log_file, 'Worker ' + identifier + ' started')

    # Register SIGTERM / SIGINT handler
    def exit_gracefully(exitcode=0):
        if os.path.isfile(ready_filename):
            os.remove(ready_filename)
        sys.exit(exitcode)

    signal.signal(signal.SIGABRT, exit_gracefully)
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    # Waiting for jobs
    output_dir = os.path.join(fib_temp_dir, identifier)
    while True:
        if not os.path.isfile(ready_filename):
            log_message(log_file, 'Token file was deleted. Worker will stop.')
            break
        if not os.path.isfile(job_filename):
            time.sleep(args.idletime)
        else:
            job_file = open(job_filename, 'r')
            abort = False
            for line in job_file:
                if not line.isspace():
                    entry = line.split(None, 1)
                    # Replace variables
                    entry[1] = entry[1].replace('$HOST$', socket.gethostname())
                    entry[1] = entry[1].replace('$INSTANCE$', args.identifier)
                    entry[1] = entry[1].replace('$OUTPUT$', output_dir)
                    entry[1] = entry[1].rstrip()
                    # Run job the requested number of times
                    for i in range(1, int(entry[0]) + 1, 1):
                        log_message(log_file, 'Executing job (' + str(i) + '/' + entry[0] + '): ' + entry[1])
                        # Flush logfile
                        log_file.flush()
                        retval = os.system(entry[1])
                        log_message(log_file, 'Finished job. Return value: ' + str(retval))
                        if retval != 0:
                            # Job failed
                            log_message(log_file, 'Job failed. Aborting whole job batch.')
                            open(os.path.join(fib_job_dir, identifier + failed_suffix), 'w').close()
                            delete_all_contents(output_dir)
                            abort = True
                            break
                    # Flush logfile
                    log_file.flush()
                    if abort:
                        break
            job_file.close()
            os.remove(job_filename)

    log_message(log_file, 'Worker ' + identifier + ' stopped')
    log_file.close()
    exit_gracefully()


def worker_stop(args):
    token_filename = os.path.join(fib_token_dir, args.identifier)
    if not os.path.isfile(token_filename):
        print_error('No worker ' + args.identifier + ' running in this working directory.')
        sys.exit(1)
    else:
        os.remove(token_filename)
        print('Worker ' + args.identifier + ' will gracefully exit after its current job.')


def worker_stopall(args):
    delete_all_contents(fib_token_dir)
    print('All worker will gracefully exit after their current job.')


def initialize(args):
    open(fib_filename, 'w').close()
    if not os.path.isdir(fib_log_dir):
        os.mkdir(fib_log_dir)
    if not os.path.isdir(fib_job_dir):
        os.mkdir(fib_job_dir)
    if not os.path.isdir(fib_temp_dir):
        os.mkdir(fib_temp_dir)
    if not os.path.isdir(fib_token_dir):
        os.mkdir(fib_token_dir)
    if not os.path.isdir(fib_result_dir):
        os.mkdir(fib_result_dir)
    print('Directory was initialized as working directory.')


def repair(args):
    print('Are you sure that no scheduler and no worker is running in this working directory? (y/n)')
    sys.stdout.write('> ')
    if get_input(True) == 'y':
        cleanup_directory(True)
        print('Repair finished.')
    else:
        print('Repair aborted.')
        sys.exit(1)


def reset(args):
    print('ATTENTION! This command will erase everything (including results and logs)!')
    randstr = id_generator(4)
    print('Confirm to proceed by typing: ' + randstr)
    sys.stdout.write('> ')
    if get_input(True) == randstr.lower():
        cleanup_directory(True)
        delete_all_contents(fib_result_dir)
        delete_all_contents(fib_log_dir)
        if os.path.isfile(fib_scheduler_continuation_filename):
            os.remove(fib_scheduler_continuation_filename)
        print('Reset finished.')
    else:
        print('Reset aborted.')
        sys.exit(1)


def startup():
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()
    # Layer 0: Scheduler command
    parser_scheduler = subparsers.add_parser('scheduler')
    assert isinstance(parser_scheduler, ArgumentParser)
    subparsers_scheduler = parser_scheduler.add_subparsers(dest='subparser_name')
    # Layer 1: Scheduler Start command
    parser_scheduler_start = subparsers_scheduler.add_parser('start')
    assert isinstance(parser_scheduler_start, ArgumentParser)
    parser_scheduler_start.set_defaults(func=scheduler_start)
    parser_scheduler_start.add_argument('jobfile', help='Path to job file or "continue"')
    parser_scheduler_start.add_argument('-e', '--exitworker', action='store_true',
                                        help='Scheduler will send exit signals to all workers after'
                                             'completing job list.')
    parser_scheduler_start.add_argument('-i', '--idletime', action='store', type=float, default=5,
                                        help='Timespan to sleep when there is nothing to to (polling interval).')
    parser_scheduler_start.add_argument('-r', '--resign', action='store', type=int, default=5,
                                        help='Number of times a job is allowed to fail before it is removed from'
                                             'scheduling.')
    # Layer 1: Scheduler Continue command
    parser_scheduler_continue = subparsers_scheduler.add_parser('continue')
    assert isinstance(parser_scheduler_continue, ArgumentParser)
    parser_scheduler_continue.set_defaults(func=scheduler_start)
    parser_scheduler_continue.add_argument('-e', '--exitworker', action='store_true',
                                           help='Scheduler will send exit signals to all workers after'
                                                'completing job list.')
    parser_scheduler_continue.add_argument('-i', '--idletime', action='store', type=float, default=5,
                                           help='Timespan to sleep when there is nothing to to (polling interval).')
    parser_scheduler_continue.add_argument('-r', '--resign', action='store', type=int, default=5,
                                           help='Number of times a job is allowed to fail before it is removed from'
                                                'scheduling.')
    # Layer 1: Scheduler Stop command
    parser_scheduler_stop = subparsers_scheduler.add_parser('stop')
    assert isinstance(parser_scheduler_stop, ArgumentParser)
    parser_scheduler_stop.set_defaults(func=scheduler_stop)
    # Layer 0: Worker command
    parser_worker = subparsers.add_parser('worker')
    assert isinstance(parser_worker, ArgumentParser)
    subparsers_worker = parser_worker.add_subparsers()
    # Layer 1: Worker Start command
    parser_worker_start = subparsers_worker.add_parser('start')
    assert isinstance(parser_worker_start, ArgumentParser)
    parser_worker_start.add_argument('identifier', type=str, default='1',
                                     help='Unique identifier on this machine')
    parser_worker_start.add_argument('-i', '--idletime', action='store', type=float, default=5,
                                     help='Timespan to sleep when there is nothing to to (polling interval).')
    parser_worker_start.set_defaults(func=worker_start)
    # Layer 1: Worker Stop command
    parser_worker_stop = subparsers_worker.add_parser('stop')
    assert isinstance(parser_worker_stop, ArgumentParser)
    parser_worker_stop.add_argument('identifier', type=str, default='1',
                                    help='Unique identifier on this machine')
    parser_worker_stop.set_defaults(func=worker_stop)
    # Layer 1: Worker Stopall command
    parser_worker_stopall = subparsers_worker.add_parser('stopall')
    assert isinstance(parser_worker_stopall, ArgumentParser)
    parser_worker_stopall.set_defaults(func=worker_stopall)
    # Layer 0: Init command
    parser_init = subparsers.add_parser('init')
    assert isinstance(parser_init, ArgumentParser)
    parser_init.set_defaults(func=initialize)
    # Layer 0: Repair command
    parser_repair = subparsers.add_parser('repair')
    assert isinstance(parser_repair, ArgumentParser)
    parser_repair.set_defaults(func=repair)
    # Layer 0: Reset command
    parser_reset = subparsers.add_parser('reset')
    assert isinstance(parser_reset, ArgumentParser)
    parser_reset.set_defaults(func=reset)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    startup()