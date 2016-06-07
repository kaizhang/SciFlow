Scientific workflow management system
=====================================

Introduction
------------

SciFlow is a workflow management system for working with big data pipelines locally
or in a grid computing system.

Most scientific computing pipelines are composed of many computational steps, and each of them involves heavy computation and IO operations. A workflow management system can
help user design complex computing patterns and track the states of computation.
The ability to recover from failures is crucial in large pipelines as they usually
take days or weeks to finish.

Features:

1. Easy to use: A simple and flexible way to specify computational pipelines in Haskell.

2. Automatic Checkpointing: The result of each intermediate step is stored, allowing easy restart upon failures.

3. Parallelism and grid computing support: Independent computational steps will run concurrently. And users can decide whether to run steps locally or on remote compute nodes in a grid system.

Here is a simple example. (Since we use template haskell, we need to divide this small program into two files.)

```haskell
---------------------------------------------------
-- File 1: MyModule.hs
---------------------------------------------------
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Functions
    (builder) where

import Control.Lens ((^.), (.=))
import qualified Data.Text as T
import Shelly hiding (FilePath)
import Text.Printf (printf)

import Scientific.Workflow

create :: () -> IO FilePath
create _ = do
    writeFile "hello.txt" "hello world"
    return "hello.txt"

countWords :: FilePath -> IO Int
countWords fl = do
    content <- readFile fl
    return $ length $ words content

countChars :: FilePath -> IO Int
countChars fl = do
    content <- readFile fl
    return $ sum $ map length $ words content

output :: (Int, Int) -> IO Bool
output (ws, cs) = do
    putStrLn $ printf "Number of words: %d" ws
    putStrLn $ printf "Number of characters: %d" cs
    return True

cleanUp :: (Bool, FilePath) -> IO ()
cleanUp (toBeRemoved, fl) = if toBeRemoved
    then shelly $ rm $ fromText $ T.pack fl
    else return ()

-- builder monad
builder :: Builder ()
builder = do
    node "step0" 'create $ label .= "write something to a file"
    node "step1" 'countWords $ label .= "word count"
    node "step2" 'countChars $ label .= "character count"
    node "step3" 'output $ label .= "print"
    node "step4" 'cleanUp $ label .= "remove the file"

    ["step0"] ~> "step1"
    ["step0"] ~> "step2"
    ["step1", "step2"] ~> "step3"
    ["step3", "step0"] ~> "step4"

---------------------------------------------------
-- File 2: main.hs
---------------------------------------------------
{-# LANGUAGE TemplateHaskell #-}

import qualified Functions as F
import Scientific.Workflow.Main

defaultMain F.builder
```

Use `ghc main.hs -threaded` to compile the program. And type `./main --help` to
see available commands. For example, the workflow can be visualized by running
`./main view | dot -Tpng > example.png`, as shown below.

![example](example.png)

To run the workflow, simply type `./main run`. The program will create a sqlite database to store intermediate results. If being terminated prematurely, the program will use the saved data to continue from the last step.

To enable grid compute engine support, you need to have DRMAA C library installed
and compile the SciFlow with `-f sge` flag. Use `./main run --remote` to submit jobs
to remote machines.
