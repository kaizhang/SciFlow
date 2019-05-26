{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
module Control.Workflow.Main where

import Data.Aeson (FromJSON)
import Control.Workflow.Coordinator
import Control.Workflow
import Data.Binary (Binary)
import           Options.Applicative

import Control.Workflow.Main.Types
import Control.Workflow.Main.Command

defaultMainWith :: (Binary env, FromJSON env)
                => [(String, Parser Options)]
                -> SciFlow env
                -> IO ()
defaultMainWith parsers flow = execParser optParser >>= \case
    Options cmd -> runCommand cmd flow
  where
    optParser = mkArgsParser "" parsers