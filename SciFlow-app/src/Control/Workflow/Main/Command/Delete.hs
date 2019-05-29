{-# LANGUAGE RecordWildCards #-}

module Control.Workflow.Main.Command.Delete (delete) where

import qualified Data.Text as T
import           Options.Applicative
import Control.Workflow.DataStore

import Control.Workflow.Main.Types

data Delete = Delete
    { jobNames :: [T.Text]
    , dbPath :: FilePath }

instance IsCommand Delete where
    runCommand Delete{..} _ = withStore dbPath $ \store ->
        mapM_ (delItems store) jobNames

delete :: Parser Command
delete = fmap Command $ Delete
    <$> (some . strArgument)
        ( metavar "NODE1 [NODE2] [NODE3]")
    <*> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> metavar "DB_PATH" )
