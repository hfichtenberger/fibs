# README #

FIBS is a simple self-contained scheduler that uses a common file system (like a network share) as communication channel between scheduler and workers. It's main purpose is to distribute experiments to different machines, run them locally and collect the results.

FIBS is written to be run with Python 2.6+. In theory, Python 2.5- should also work if argparse.py is available but this is not thoroughly tested.