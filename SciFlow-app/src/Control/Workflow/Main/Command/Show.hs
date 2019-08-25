{-# LANGUAGE RecordWildCards #-}

module Control.Workflow.Main.Command.Show (show') where

import qualified Data.Text as T
import Control.Arrow
import Data.Yaml (decodeFileThrow)
import Control.Monad (when)
import Control.Arrow.Free (eval)
import Data.Binary(Binary)
import           Options.Applicative
import Control.Workflow.DataStore (mkKey, withStore, fetchItem, DataStore(..))
import Control.Workflow.Types
import Text.Show.Pretty (pPrint)

import Control.Workflow.Main.Types

data Show' = Show'
    { jobName :: T.Text
    , dbPath :: FilePath
    , raw :: Bool }

instance IsCommand Show' where
    runCommand Show'{..} flow = withStore dbPath $ \store ->
        runKleisli (showFlow jobName store flow) ()

show' :: Parser Command
show' = fmap Command $ Show'
    <$> strArgument (metavar "NODE")
    <*> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> metavar "DB_PATH" )
    <*> switch
        ( long "raw"
       <> help "show raw content. (default: False)" )

showFlow :: Binary env
         => T.Text
         -> DataStore
         -> SciFlow env
         -> Kleisli IO () ()
showFlow jn store sciflow = eval (Kleisli . runFlow') $ _flow sciflow
  where
    runFlow' (Step job) = runKleisli $ eval ( \(Action _) ->
        Kleisli $ \i -> do
            let key = mkKey i $ _job_name job
            res <- fetchItem store key
            when (_job_name job == jn) $ pPrint res
            return res
        ) $ _job_action job