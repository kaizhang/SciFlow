{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
module Control.Workflow.Main
    ( defaultMain
    , SubParser(..)
    , runParser
    , viewParser
    , remoteParser
    , deleteParser
    , showParser
    ) where

import Data.Aeson (FromJSON)
import Control.Workflow.Coordinator
import Control.Workflow
import Data.Binary (Binary)
import           Options.Applicative

import Control.Workflow.Main.Types
import Control.Workflow.Main.Command

-- | Make app with default the argument parser.
defaultMain :: (Binary env, FromJSON env)
            => String   -- ^ Program header
            -> [SubParser Command]  -- ^ Commands
            -> SciFlow env
            -> IO ()
defaultMain h parsers flow = customExecParser modifier argParser >>= \case 
    Command cmd -> runCommand cmd flow
  where
    argParser = mkArgsParser h parsers
    modifier = prefs showHelpOnEmpty