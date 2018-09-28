# README

FIBS is a simple, self-contained job scheduler that uses a common file system (like a network share) as communication channel between scheduler and workers. It's main purpose is to distribute algorithmic experiments to machines, run them locally and collect the results. The integrated help system `fibs.py -h` provides a references and includes a walk through that can be accessed by `fibs.py help`.

FIBS is written to be run with Python 2.6+. In theory, Python 2.5- should also work if argparse.py is available but this is not thoroughly tested. Since this tool was develop for an ancient environment, Python 3 is not supported. However, it should not be hard to migrate it.
