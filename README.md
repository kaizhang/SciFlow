Scientific workflow management system
=====================================

I'm in the process of redesigning the framework using Cloud Haskell and free arrows.

Introduction
------------

SciFlow is a DSL for building scientific workflows. Workflows built with SciFlow
can be run either on normal desktops or in grid computing environments that
support DRMAA.

Most scientific computing pipelines are composed of many computational steps, and each of them involves heavy computation and IO operations. A workflow management system can
help user design complex computing patterns and track the states of computation.
The ability to recover from failures is crucial in large pipelines as they usually
take days or weeks to finish.

Features
--------

1. Easy to use and safe: Provide a simple and flexible way to design type safe
computational pipelines in Haskell.

2. Automatic Checkpointing: The states of intermediate steps are automatically
logged, allowing easy restart upon failures.

3. Parallelism and grid computing support.

Examples
--------

See examples in the "examples" directory for more details.

Use `ghc main.hs -threaded` to compile the examples.
And type `./main --help` to see available commands.

To run the workflow, simply type `./main run`. The program will create a sqlite database to store intermediate results. If being terminated prematurely, the program will use the saved data to continue from the last step.

To enable grid compute engine support, you need to have DRMAA C library
installed and compile the SciFlow with `-f drmaa` flag.
Use `./main run --remote` to submit jobs to remote machines.

Featured applications
--------------------

[Here](https://github.com/Taiji-pipeline)
are some bioinformatics pipelines built with SciFlow.
