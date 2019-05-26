{-# LANGUAGE GADTs #-}
module Control.Workflow.Main.Types where

import Data.Aeson (FromJSON)
import Data.Binary (Binary)

import           Options.Applicative
import Control.Workflow.Coordinator
import Control.Workflow

class Command a where
    runCommand :: (FromJSON env, Binary env)
               => a
               -> SciFlow env
               -> IO ()

data Options where
    Options :: Command a => a -> Options