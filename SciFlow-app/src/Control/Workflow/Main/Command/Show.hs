{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE BangPatterns #-}

module Control.Workflow.Main.Command.Show (show') where

import qualified Data.Text as T
import qualified Data.Text.IO as T
import Control.Arrow
import Control.Monad (forM_)
import Control.Arrow.Free (eval)
import Data.Binary(Binary)
import           Options.Applicative
import Control.Workflow.DataStore (Key(..), mkKey, withStore, fetchItem, DataStore(..), queryStatus)
import Control.Workflow.Types
import qualified Data.HashMap.Strict as M
import Text.Show.Pretty (ppShow)
import Control.Exception (handle, SomeException(..))
import Control.Concurrent.MVar
import Data.List
import Data.Ord

import Control.Workflow.Main.Types

data Show' = Show'
    { stepName :: Maybe T.Text
    , dbPath :: FilePath }

instance IsCommand Show' where
    runCommand Show'{..} flow = withStore dbPath $ \store -> do
        cache <- newMVar M.empty
        handle (\(SomeException _) -> return ()) $ runKleisli (showFlow cache store flow) ()
        takeMVar cache >>= printCache . M.filterWithKey f
      where
        f k _ = case stepName of
            Nothing -> True
            Just nm -> _name k == nm
    

printCache :: M.HashMap Key T.Text -> IO ()
printCache cache = forM_ records $ \(k, x) -> do
    T.putStrLn $ "<< " <> k  <> " >>"
    T.putStrLn $ x <> "\n"
  where
    records = sortBy (comparing fst) $ map (\(a,b) -> (T.pack $ show a, b)) $
        M.toList cache

show' :: Parser Command
show' = fmap Command $ Show'
    <$> (optional . strArgument)
        ( metavar "NODE"
       <> help "The name of the step" ) 
    <*> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> metavar "DB_PATH" )

showFlow :: Binary env
         => MVar (M.HashMap Key T.Text)
         -> DataStore
         -> SciFlow env
         -> Kleisli IO () ()
showFlow cache store sciflow = eval (Kleisli . runFlow') $ _flow sciflow
  where
    runFlow' (Step job) = runKleisli $ eval ( \(Action _) ->
        Kleisli $ \ ~i -> handle (\(SomeException _) -> return undefined) $ do
            let key = mkKey i $ _job_name job
            queryStatus store key >>= \case
                Nothing -> return undefined
                Just _ -> do
                    res <- fetchItem store key
                    modifyMVar_ cache $ return . M.insert key (T.pack $ ppShow res)
                    return res
        ) $ _job_action job