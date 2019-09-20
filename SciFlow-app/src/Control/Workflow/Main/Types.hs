{-# LANGUAGE GADTs #-}
module Control.Workflow.Main.Types where

import Data.Aeson (FromJSON)
import Data.Binary (Binary)

import Control.Workflow

class IsCommand a where
    runCommand :: (FromJSON env, Binary env)
               => a
               -> SciFlow env
               -> IO ()

data Command where
    Command :: IsCommand a => a -> Command