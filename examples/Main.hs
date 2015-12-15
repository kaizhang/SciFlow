{-# LANGUAGE TemplateHaskell #-}

import System.Environment

import qualified Functions as F
import qualified Data.Text.Lazy.IO as T

import Scientific.Workflow
import Scientific.Workflow.Visualize

buildWorkflow "wf" F.builder

main :: IO ()
main = do
    (cmd:args) <- getArgs
    case cmd of
        "run" -> runWorkflow wf def
        "view" -> T.putStrLn $ renderBuilder F.builder
