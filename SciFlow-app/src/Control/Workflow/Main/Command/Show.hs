{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}

module Control.Workflow.Main.Command.Show (show') where

import qualified Data.Text as T
import Control.Arrow
import Data.Yaml (decodeFileThrow)
import Control.Monad (when)
import Control.Arrow.Free (eval)
import Data.Binary(Binary)
import           Options.Applicative
import Control.Workflow.DataStore (mkKey, withStore, fetchItem, DataStore(..), queryStatus)
import Control.Workflow.Types
import Text.Show.Pretty (ppShow)
import Control.Monad.Except (ExceptT, throwError, runExceptT)
import Control.Monad.Trans (lift)

import Control.Workflow.Main.Types

data Show' = Show'
    { dbPath :: FilePath }

instance IsCommand Show' where
    runCommand Show'{..} flow = withStore dbPath $ \store -> do
        _ <- runExceptT $ runKleisli (showFlow store flow) ()
        return ()

show' :: Parser Command
show' = fmap Command $ Show'
    <$> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> metavar "DB_PATH" )

showFlow :: Binary env
         => DataStore
         -> SciFlow env
         -> Kleisli (ExceptT () IO) () ()
showFlow store sciflow = eval (Kleisli . runFlow') $ _flow sciflow
  where
    runFlow' (Step job) = runKleisli $ eval ( \(Action _) ->
        Kleisli $ \i -> do
            let key = mkKey i $ _job_name job
            lift (queryStatus store key) >>= \case
                Nothing -> throwError ()
                Just _ -> lift $ do
                    res <- fetchItem store key
                    putStrLn $ show key <> ": " <> ppShow res
                    return res
        ) $ _job_action job