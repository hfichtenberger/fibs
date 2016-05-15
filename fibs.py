#!/usr/bin/env python

"""FIBS - a file-based scheduler"""

try:
	import argparse
except (ImportError, NameError):
	print('It seems that you are using an old version of Python. Please download argparse.py from argparse package and place it in the same directory as this file. At the moment of this writing, it is available at:')
	print('https://pypi.python.org/pypi/argparse')
	sys.exit(1)


import datetime
import logging
import os
import random
import signal
import socket
import string
import subprocess
import sys
import textwrap
import time


__author__ = 'Hendrik Fichtenberger'
__version__ = '1.0.0'
__email__ = 'firstname.lastname@tu-dortmund.de'

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
	def __init__(self, total_runs, run_per_assignment, command):
		self.total_runs = total_runs if total_runs is not None else 0
		self.run_per_assignment = run_per_assignment if run_per_assignment is not None else 1
		self.command = command if command is not None else ''


class JobState(Job):
	def __init__(self, total_runs, run_per_assignment, command, remaining_runs = -1, fails = 0, in_execution = 0):
		Job.__init__(self, total_runs, run_per_assignment, command)
		if remaining_runs == -1:
			remaining_runs = total_runs
		self.remaining_runs = remaining_runs if remaining_runs is not None else self.total_runs
		self.fails = fails if fails is not None else 0
		self.in_execution = in_execution if in_execution is not None else 0


class Jobs:
	def __init__(self):
		self.jobs = list()

	def append(self, job):
		return self.jobs.append(job)

	def remove(self, job):
		return self.jobs.remove(job)

	def contains(self, job):
		return job in self.jobs

	def pop(self, job):
		return self.jobs.pop(job)

	def count(self):
		return len(self.jobs)

	def iter(self):
		for job in self.jobs:
			yield job


class Workers:
	def __init__(self):
		self.all_workers = set()
		self.idle_workers = set()
		self.busy_workers = dict()

	def add(self, worker):
		self.all_workers.add(worker)
		self.idle_workers.add(worker)

	def remove(self, worker):
		self.idle_workers.remove(worker)
		self.all_workers.remove(worker)

	def assign(self, worker, payload):
		self.idle_workers.remove(worker)
		self.busy_workers[worker] = payload

	def rearm(self, worker):
		del self.busy_workers[worker]
		self.idle_workers.add(worker)

	def is_known(self, worker):
		return worker in self.idle_workers or worker in self.busy_workers

	def copy_of_all_workers(self):
		return self.all_workers.copy()

	def has_unassigned_worker(self):
		return len(self.idle_workers) > 0

	def iter_unassigned_workers(self):
		for worker in self.idle_workers:
			yield worker;

	def has_assigned_worker(self):
		return len(self.busy_workers)

	def iter_assigned_workers(self):
		for worker in self.busy_workers:
			yield (worker, self.busy_workers[worker])

	def get_assigned_runbatch(self, worker):
		return self.busy_workers[worker]


class RunBatch:
	def __init__(self, job, number_of_runs):
		self.job = job
		self.number_of_runs = number_of_runs


class Scheduler:
	def __init__(self, resign_threshold):
		self.workers = Workers()
		self.active_jobs = Jobs()
		self.failed_jobs = Jobs()
		self.finished_jobs = Jobs()
		self.resign_threshold = resign_threshold

	def enqueue_job(self, job):
		self.active_jobs.append(job)

	def update_workers(self, workers):
		all_workers = self.workers.copy_of_all_workers()
		new_workers = workers - all_workers
		terminated_workers = all_workers - workers
		for worker in new_workers:
			self.workers.add(worker)
		for worker in terminated_workers:
			self.workers.remove(worker)
		return new_workers, terminated_workers

	def schedule(self):
		# Schedule jobs
		while self.workers.has_unassigned_worker():
			# Get next job
			try:
				job = next(job for job in self.active_jobs.iter() if job.remaining_runs > 0)
			except StopIteration:
				return
			# Get worker and assign job
			worker = next(self.workers.iter_unassigned_workers())
			num_assigned_jobs = min(job.run_per_assignment, job.remaining_runs)
			job.remaining_runs -= num_assigned_jobs
			job.in_execution += 1
			run_batch = RunBatch(job, num_assigned_jobs)
			self.workers.assign(worker, run_batch)
			yield worker, run_batch

	def rearm(self, worker, successful):
		run_batch = self.workers.get_assigned_runbatch(worker)
		job = run_batch.job
		job.in_execution -= 1
		self.workers.rearm(worker)
		if successful:
			if job.remaining_runs == 0 and job.in_execution == 0:
				self.active_jobs.remove(job)
				self.finished_jobs.append(job)
				return True, job
			return False, job
		else:
			job.remaining_runs += run_batch.number_of_runs
			job.fails += 1
			if job.fails >= self.resign_threshold and self.active_jobs.contains(job):
				self.active_jobs.remove(job)
				self.failed_jobs.append(job)
				return True, job
			return False, job

	def iter_assigned_workers(self):
		for worker, run_batch in self.workers.iter_assigned_workers():
			yield worker, run_batch

	def has_assigned_workers(self):
		return self.workers.has_assigned_worker()

	def is_done(self):
		return not self.has_assigned_workers() and self.active_jobs.count() == 0

	def count_remaining(self):
		return self.active_jobs.count() + self.failed_jobs.count()

	def iter_remaining(self):
		for job in self.active_jobs.iter():
			yield job
		for job in self.failed_jobs.iter():
			yield job


def print_error(message):
	print("ERROR: " + message)


def get_input(lowercase=False):
	answer = raw_input().strip()
	answer = answer.lower() if lowercase else answer
	return answer


def assert_working_dir():
	if not os.path.isfile(fib_filename):
		print_error('Working directory was not initialized.')
		sys.exit(1)


def build_identifier(a, b):
	return a + '_' + b


def generator_random_id(size=6, chars=string.ascii_uppercase + string.digits):
	return ''.join(random.choice(chars) for _ in range(size))


def delete_all_contents(directory):
	for current_dir_name, dir_names, filenames in os.walk(directory, topdown=False):
		for filename in filenames:
			os.remove(os.path.join(current_dir_name, filename))
		for dirname in dir_names:
			os.rmdir(os.path.join(current_dir_name, dirname))


def cleanup_directory(clean_state_files=False):
	delete_all_contents(fib_job_dir)
	delete_all_contents(fib_temp_dir)
	if clean_state_files:
		delete_all_contents(fib_token_dir)
		if os.path.isfile(fib_scheduler_running_filename):
			os.remove(fib_scheduler_running_filename)


def read_jobs(filename):
	job_file = open(filename, 'r')
	for line in job_file:
		if not line.isspace():
			splitted = line.split(None, 2)
			job = JobState(int(splitted[0]), int(splitted[1]), splitted[2].rstrip())
			yield job
	job_file.close()

def write_jobs(filename, jobs, append = False):
	remaining_jobs_file = open(fib_scheduler_continuation_filename, 'w' if not append else 'a')
	for job in jobs:
		remaining_jobs_file.write(str(job.remaining_runs) + ' ' + str(job.run_per_assignment) + ' ' + str(job.command) + '\n')

def stop_scheduler():
	if os.path.isfile(fib_scheduler_running_filename):
		os.remove(fib_scheduler_running_filename)


def stop_worker(token_filename):
	os.remove(token_filename)


def stop_all_workers():
	delete_all_contents(fib_token_dir)


def initialize():
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


def repair():
	cleanup_directory(True)


def send_final_mail(address, job_filename, has_finished):
	email_title = 'FIBS '
	email_msg = job_filename + '\n'
	if has_finished:
		email_title += 'PAUSED: '
		email_msg += 'Job scheduling has been PAUSED.'
	else:
		os.remove(fib_scheduler_continuation_filename)
		email_title += 'FINISHED: '
		email_msg += 'All jobs have been FINISHED.'
	email_title += job_filename

	try:
		mailhandle = subprocess.Popen('mail -s "' + email_title + '" "' + address + '"', stdin=subprocess.PIPE, shell=True)
		mailhandle.communicate(email_msg)
		if mailhandle.wait() == 0:
			logging.info('Sent mail to operator.')
		else:
			logging.error('Sending mail to operator failed.')
	except OSError:
		logging.error('Sending mail to operator failed.')


def run_scheduler_start(args):
	# Initialize logging
	logging.basicConfig(filename=os.path.join(fib_log_dir, 'scheduler.txt'), format='%(asctime)s %(message)s', level=logging.INFO)

	# Register SIGABRT / SIGTERM / SIGINT handler
	def exit_gracefully(exitcode=0, frame=None):
		cleanup_directory()
		stop_scheduler()
		sys.exit(exitcode)
	signal.signal(signal.SIGABRT, exit_gracefully)
	signal.signal(signal.SIGINT, exit_gracefully)
	signal.signal(signal.SIGTERM, exit_gracefully)

	# Check setup
	assert_working_dir()
	if os.path.isfile(fib_scheduler_running_filename):
		print_error('Scheduler is already running or has crashed. In case of the latter, consider the repair option.')
		sys.exit(1)

	# Cleanup remainings from ungraceful terminations
	cleanup_directory()

	# Get configuration
	if args.subparser_name == 'start':
		if os.path.isfile(fib_scheduler_continuation_filename):
			print_error('Please remove the continuation file "' + fib_scheduler_continuation_filename + '" to confirm that you want to start over. You might also want to delete existing results and logs by using the reset command.')
			exit_gracefully(1)
		else:
			if not os.path.isfile(args.jobfile):
				print_error('Job file "' + args.jobfile + '" does not exist.')
				exit_gracefully(1)
			job_filename = args.jobfile
	else:
		if os.path.isfile(fib_scheduler_continuation_filename):
			job_filename = fib_scheduler_continuation_filename
		else:
			print_error('No continuation file.')
			exit_gracefully(1)

	# Read configuration and start scheduler
	scheduler = Scheduler(args.resign)
	jobs = read_jobs(job_filename)
	for job in jobs:
		scheduler.enqueue_job(job)
	open(fib_scheduler_running_filename, 'w').close()
	logging.info('Scheduler started')

	# Schedule
	attempt_to_shutdown = False
	while True:
		# Check for busy -> idle workers
		iterate_busy_workers = True
		while(iterate_busy_workers):
			iterate_busy_workers = False
			for worker, run_batch in scheduler.iter_assigned_workers():
				if not os.path.isfile(os.path.join(fib_job_dir, worker)):
					# Construct file paths
					out_dir = os.path.join(fib_temp_dir, worker)
					failed_filepath = os.path.join(fib_job_dir, worker + failed_suffix)
					if not os.path.isfile(failed_filepath):
						# Job successful
						removed, job = scheduler.rearm(worker, True)
						logging.info('Idle worker: Job completed on ' + worker + ': ' + job.command)
						if removed:
							logging.info('Job finished: ' + job.command)
						# Move results
						for filename in os.listdir(out_dir):
							from_fullname = os.path.join(out_dir, filename)
							if os.path.isfile(from_fullname):
								to_fullname = os.path.join(fib_result_dir, filename)
								if os.path.isfile(to_fullname):
									to_fullname += '_' + generator_random_id()
								os.rename(from_fullname, to_fullname)
					else:
						# Job failed
						removed, job = scheduler.rearm(worker, False)
						logging.info('Idle worker: Job failed on ' + worker + ': ' + job.command)
						if removed:
							logging.warning('Job removed from schedule due to too many failed runs: ' + job.command)
						# Remove fail state indicator file
						os.remove(failed_filepath)
					iterate_busy_workers = True
					break

		# Check for new workers and idle -> terminated workers
		ready_files = set(filename for filename in os.listdir(fib_token_dir) if os.path.isfile(os.path.join(fib_token_dir, filename)))
		new_workers, terminated_workers = scheduler.update_workers(ready_files)
		for worker in new_workers:
			logging.info('New worker: ' + worker)
			temp_dirname = os.path.join(fib_temp_dir, worker)
			if not os.path.isdir(temp_dirname):
				os.mkdir(temp_dirname)
		for worker in terminated_workers:
			logging.info('Terminated worker: ' + worker)

		# Schedule
		if not attempt_to_shutdown:
			for worker, run_batch in scheduler.schedule():
				# Prepare preliminary output directory
				out_dir = os.path.join(fib_temp_dir, worker)
				if os.path.isdir(out_dir):
					delete_all_contents(out_dir)
				else:
					os.mkdir(out_dir)
				# Prepare job file
				temp_jobfilename = os.path.join(fib_job_dir, generator_random_id())
				real_jobfilename = os.path.join(fib_job_dir, worker)
				temp_jobfile = open(temp_jobfilename, 'w')
				temp_jobfile.write(str(run_batch.number_of_runs) + ' ' + run_batch.job.command + '\n')
				temp_jobfile.close()
				# Go worker!
				os.rename(temp_jobfilename, real_jobfilename)
				logging.info('Busy worker: Job scheduled on worker ' + worker + ": " + run_batch.job.command)

		# Scheduler token file removed?
		if not os.path.isfile(fib_scheduler_running_filename) and not attempt_to_shutdown:
			logging.info('Token file was deleted. Scheduler will stop after all workers completed.')
			attempt_to_shutdown = True
		# No busy workers means we are done
		if (attempt_to_shutdown and not scheduler.has_assigned_workers()) or scheduler.is_done():
			break
		# Idle-Sleep
		time.sleep(args.idletime)

	# Write remaining jobs into file
	if os.path.isfile(fib_scheduler_continuation_filename):
		os.remove(fib_scheduler_continuation_filename)
	number_remaining = scheduler.count_remaining()
	if number_remaining > 0:
		write_jobs(fib_scheduler_continuation_filename, scheduler.iter_remaining())
		logging.info('Wrote continuation file because there are ' + str(number_remaining) + ' remaining jobs.')
	if args.sendmail is not None:
		send_final_mail(args.sendmail, job_filename, number_remaining == 0)

	# Exit workers?
	if args.exitworker:
		stop_all_workers()

	logging.info('Scheduler stopped')
	exit_gracefully()


def run_scheduler_stop(args):
	if not os.path.isfile(fib_scheduler_running_filename):
		print_error('No scheduler running in this working directory.')
		sys.exit(1)
	else:
		stop_scheduler()
		print('The scheduler will gracefully exit after all running workers finished their current jobs.')


def run_worker_start(args):
	# Get worker identifier
	identifier = build_identifier(socket.gethostname(), args.identifier)
	ready_filename = os.path.join(fib_token_dir, identifier)
	# Initialize logging
	logging.basicConfig(filename=os.path.join(fib_log_dir, identifier + '.txt'), format='%(asctime)s %(message)s', level=logging.INFO)

	# Register SIGABRT / SIGTERM / SIGINT handler
	def exit_gracefully(exitcode=0, frame=None):
		if os.path.isfile(ready_filename):
			os.remove(ready_filename)
		sys.exit(exitcode)
	signal.signal(signal.SIGABRT, exit_gracefully)
	signal.signal(signal.SIGINT, exit_gracefully)
	signal.signal(signal.SIGTERM, exit_gracefully)

	# Boot worker
	assert_working_dir()
	if os.path.isfile(ready_filename):
		print_error('A worker with the ID ' + args.identifier + ' is already in running or has crashed. In case of the latter, consider the repair option.')
		sys.exit(1)
	open(ready_filename, 'w').close()
	job_filename = os.path.join(fib_job_dir, identifier)
	logging.info('Worker ' + identifier + ' started')

	# Waiting for jobs
	output_dir = os.path.join(os.getcwd(), fib_temp_dir, identifier)
	while True:
		if not os.path.isfile(ready_filename):
			logging.info('Token file was deleted. Worker will stop.')
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
						logging.info('Executing job (' + str(i) + '/' + entry[0] + '): ' + entry[1])
						process = subprocess.Popen(entry[1], shell=True)
						process.wait()
						logging.info('Finished job. Return value: ' + str(process.returncode))
						if process.returncode != 0:
							# Job failed
							logging.info('Job failed. Aborting whole job batch.')
							open(os.path.join(fib_job_dir, identifier + failed_suffix), 'w').close()
							delete_all_contents(output_dir)
							abort = True
							break
					if abort:
						break
			job_file.close()
			os.remove(job_filename)

	logging.info('Worker ' + identifier + ' stopped')
	exit_gracefully()


def run_worker_stop(args):
	token_filename = os.path.join(fib_token_dir, args.hostname_identifier)
	if not os.path.isfile(token_filename):
		identifier = build_identifier(socket.gethostname(), args.identifier)
		guess_filename = os.path.join(fib_token_dir, identifier + '_' + args.hostname_identifier)
		tip = ''
		if os.path.isfile(guess_filename):
			tip = "\nDid you mean '" + guess_filename + "'?"
		print_error('No worker ' + args.hostname_identifier + ' running in this working directory.' + tip)
		sys.exit(1)
	else:
		stop_worker(token_filename)
		print('Worker ' + args.hostname_identifier + ' will gracefully exit after its current job.')


def run_worker_stopall(args):
	stop_all_workers()
	print('All worker will gracefully exit after their current job.')


def run_initialize(args):
	initialize()
	print('Directory was initialized as working directory.')


def run_repair(args):
	print('Are you sure that no scheduler and no worker is running in this working directory? (y/n)')
	sys.stdout.write('> ')
	if get_input(True) == 'y':
		repair()
		print('Repair finished.')
	else:
		print('Repair aborted.')
		sys.exit(1)


def run_reset(args):
	print('ATTENTION! This command will erase everything (including results and logs)!')
	randstr = generator_random_id(4)
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


def run_help(args):
	w = textwrap.TextWrapper(width=args.width)
	print(w.fill('FIBS is a self-contained python script for scheduling a bunch of user-defined commands ("jobs") on one or more computers. There are two main modes: "scheduler" mode and "worker" mode. In scheduler mode, FIBS processes a job file supplied by the user (you!) and schedules all contained jobs to available workers. In worker mode, FIBS accepts and executes jobs deployed by the scheduler. Multiple workers running on the same or on different machines can connect to the same scheduler. The medium of communication used to distribute jobs and receive results is a shared directory, e.g. a subdirectory of your home directory.'))
	print('')
	print(w.fill('# Job file'))
	print(w.fill('The job file uses a simple text format where each line corresponds to a job. The line format is:'))
	print(w.fill('<n> <m> <cmd>'))
	print(w.fill('This will instruct the scheduler to schedule the command <cmd> to available workers <n> times in batches of size <m>. For example'))
	print(w.fill('10 2 echo "Hello world!"'))
	print(w.fill('would schedule the echo command to available workers five times, and each time a worker would print "Hello world!" twice. If a job fails, i.e., its exit code differs from 0, the whole batch is discarded and queued for schedule again.'))
	print(w.fill('The commands may contain placeholders:'))
	w.subsequent_indent = '  '
	print(w.fill('- $HOST$ will be replaced by the worker\'s hostname'))
	print(w.fill('- $INSTANCE$ will be replaced by the worker\'s identifier which is unique amongst all workers running on the same host'))
	print(w.fill('- $OUTPUT$ will be replaced by the full path to a directory whose top-level files are collected by the scheduler after the job (batch) was successfully executed'))
	w.subsequent_indent = ''
	print('')
	print(w.fill('# Basic usage'))
	print(w.fill('Initialize a directory as working directory by running'))
	w.subsequent_indent = '  '
	print(w.fill('> python ' + sys.argv[0] + ' init'))
	w.subsequent_indent = ''
	print(w.fill('inside of it. Write a job file. Start the scheduler inside the working directory by executing'))
	w.subsequent_indent = '  '
	print(w.fill('> python ' + sys.argv[0] + ' scheduler start <jobfile>'))
	w.subsequent_indent = ''
	print(w.fill('and start one or more workers inside the working directory by running'))
	w.subsequent_indent = '  '
	print(w.fill('> python ' + sys.argv[0] + ' worker <id>'))
	w.subsequent_indent = ''
	print(w.fill('where <id> is a identifier which has to be unique amongst all workers running on the same host.'))
	print(w.fill('You may start workers on machines that differ from the one running the scheduler. The only requirement is that the working directory must be accessible. For runtime measurements, you should consider using a separate machine for scheduling.'))
	print(w.fill('More commands exist, and help texts are available through the -h / --help commandline argument. Example:'))
	w.subsequent_indent = '  '
	print(w.fill('> python ' + sys.argv[0] + ' -h scheduler'))
	print(w.fill('> python ' + sys.argv[0] + ' worker -h start'))
	w.subsequent_indent = ''
	print('')
	print(w.fill('# Directory structure'))
	print(w.fill('Results are collected in the "' + fib_result_dir + '" directory. Logs are placed in the "' + fib_log_dir + '" directory. "' + fib_token_dir + '" is used by the workers to announce their presence to the scheduler, "' + fib_job_dir + '" is used by the scheduler to deploy jobs the workers and "' + fib_temp_dir + '" is the worker output directory from where the scheduler moves the result files to the results directory after a job finished successfully, i.e., its exit code equals 0. The file "' + fib_scheduler_running_filename + '" announces that a scheduler is running in the working directory. Continuation files are named "' + fib_scheduler_continuation_filename + '".'))
	print('')
	print(w.fill('# Advanced commands'))
	print(w.fill('There is a "repair" command to repair corrupted working directories, e.g. after a worker crashed which will hopefully never happen!). Please make sure that no scheduler and no workers are running in the working directory before using it.'))
	print(w.fill('Use the "reset" command to wipe all data in your working directory. Be aware that this will delete all results and logs, too! To keep you safe from harm, there is a preceding test of your soundness of mind before anything is touched.'))


def startup():
	if sys.version_info.major >= 3 :
		print_error("This script does not support Python 3+ and might fail at any time.")
	
	parser = argparse.ArgumentParser(description='FIBS - a file-based scheduler', formatter_class=argparse.RawDescriptionHelpFormatter)
	subparsers = parser.add_subparsers()
	# Layer 0: Scheduler command
	parser_scheduler = subparsers.add_parser('scheduler', help='scheduler mode')
	assert isinstance(parser_scheduler, argparse.ArgumentParser)
	subparsers_scheduler = parser_scheduler.add_subparsers(dest='subparser_name')
	# Layer 1: Scheduler Start command
	parser_scheduler_start = subparsers_scheduler.add_parser('start', help='start scheduler')
	assert isinstance(parser_scheduler_start, argparse.ArgumentParser)
	parser_scheduler_start.set_defaults(func=run_scheduler_start)
	parser_scheduler_start.add_argument('jobfile', help='path to job file')
	parser_scheduler_start.add_argument('-e', '--exitworker', action='store_true', help='scheduler will send exit signals to all workers after completing job list.')
	parser_scheduler_start.add_argument('-i', '--idletime', action='store', type=float, default=5, help='timespan to sleep when there is nothing to to (polling interval).')
	parser_scheduler_start.add_argument('-r', '--resign', action='store', type=int, default=5, help='number of times a job is allowed to fail before it is removed from scheduling.')
	parser_scheduler_start.add_argument('-m', '--sendmail', action='store', type=str, help='a mail will be sent to this address when scheduling is paused or finished. Uses the "mail" command. ')
	# Layer 1: Scheduler Continue command
	parser_scheduler_continue = subparsers_scheduler.add_parser('continue', help='continue previously paused job processing')
	assert isinstance(parser_scheduler_continue, argparse.ArgumentParser)
	parser_scheduler_continue.set_defaults(func=run_scheduler_start)
	parser_scheduler_continue.add_argument('-e', '--exitworker', action='store_true', help='scheduler will send exit signals to all workers after completing job list.')
	parser_scheduler_continue.add_argument('-i', '--idletime', action='store', type=float, default=5, help='timespan to sleep when there is nothing to to (scheduling interval).')
	parser_scheduler_continue.add_argument('-r', '--resign', action='store', type=int, default=5, help='number of times a job is allowed to fail before it is removed from scheduling.')
	parser_scheduler_continue.add_argument('-m', '--sendmail', action='store', type=str, help='a mail will be sent to this address when scheduling is paused or finished. Uses the "mail" command. ')
	# Layer 1: Scheduler Stop command
	parser_scheduler_stop = subparsers_scheduler.add_parser('stop', help='stop gracefully after all busy worker have finished and write remaining jobs to continuation file')
	assert isinstance(parser_scheduler_stop, argparse.ArgumentParser)
	parser_scheduler_stop.set_defaults(func=run_scheduler_stop)
	# Layer 0: Worker command
	parser_worker = subparsers.add_parser('worker', help='worker mode')
	assert isinstance(parser_worker, argparse.ArgumentParser)
	subparsers_worker = parser_worker.add_subparsers()
	# Layer 1: Worker Start command
	parser_worker_start = subparsers_worker.add_parser('start', help='start Worker')
	assert isinstance(parser_worker_start, argparse.ArgumentParser)
	parser_worker_start.add_argument('identifier', type=str, default='1', help='unique identifier on this machine')
	parser_worker_start.add_argument('-i', '--idletime', action='store', type=float, default=5, help='timespan to sleep when there is nothing to to (job polling interval).')
	parser_worker_start.set_defaults(func=run_worker_start)
	# Layer 1: Worker Stop command
	parser_worker_stop = subparsers_worker.add_parser('stop', help='stop gracefully after current job')
	assert isinstance(parser_worker_stop, argparse.ArgumentParser)
	parser_worker_stop.add_argument('hostname_identifier', type=str, default='1', help='hostname of the worker\'s machine followed by an underscore and the worker\'s unique identifier')
	parser_worker_stop.set_defaults(func=run_worker_stop)
	# Layer 1: Worker Stopall command
	parser_worker_stopall = subparsers_worker.add_parser('stopall', help='stop all workers gracefully after their current jobs')
	assert isinstance(parser_worker_stopall, argparse.ArgumentParser)
	parser_worker_stopall.set_defaults(func=run_worker_stopall)
	# Layer 0: Init command
	parser_init = subparsers.add_parser('init', help='initialize current directory as working directory')
	assert isinstance(parser_init, argparse.ArgumentParser)
	parser_init.set_defaults(func=run_initialize)
	# Layer 0: Repair command
	parser_repair = subparsers.add_parser('repair', help='repair working directory')
	assert isinstance(parser_repair, argparse.ArgumentParser)
	parser_repair.set_defaults(func=run_repair)
	# Layer 0: Reset command
	parser_reset = subparsers.add_parser('reset', help='reset working directory (wipe all data)')
	assert isinstance(parser_reset, argparse.ArgumentParser)
	parser_reset.set_defaults(func=run_reset)
	# Layer 0: Help command
	parser_help = subparsers.add_parser('help', help='show help')
	assert isinstance(parser_help, argparse.ArgumentParser)
	parser_help.add_argument('-w', '--width', type=int, default='80', help='line width of help text')
	parser_help.set_defaults(func=run_help)

	# Parse arguments
	args = parser.parse_args()
	args.func(args)


if __name__ == '__main__':
	startup()
