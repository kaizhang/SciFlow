{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE BangPatterns #-}

module Control.Workflow.Main.Command.Show (show') where

import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Control.Arrow.Async
import Control.Monad (forM_)
import Control.Arrow.Free (eval)
import Data.Binary (Binary, decode)
import           Options.Applicative
import Control.Workflow.DataStore
import Control.Workflow.Types
import qualified Data.HashMap.Strict as M
import Text.Show.Pretty (ppShow)
import Control.Monad.Except
import Control.Monad.Reader (runReaderT)
import Control.Concurrent.MVar
import Data.List
import Data.Ord

import Control.Workflow.Main.Types

data Show' = Show'
    { stepName :: Maybe T.Text
    , dbPath :: FilePath }

instance IsCommand Show' where
    runCommand Show'{..} flow = withStore dbPath $ \store -> readEnv store >>= \case
        Nothing -> return ()
        Just env -> do
            cache <- newMVar M.empty
            _ <- runExceptT $ runAsyncA (showFlow cache store env flow) ()
            takeMVar cache >>= printCache . M.filterWithKey f
      where
        f k _ = case stepName of
            Nothing -> True
            Just nm -> _name k == nm
    
printCache :: M.HashMap Key T.Text -> IO ()
printCache cache = forM_ records $ \(k, x) -> do
    T.putStrLn $ "<< " <> k <> " >>"
    T.putStrLn $ x <> "\n"
  where
    records = sortBy (comparing fst) $ map (\(a,b) -> (T.pack $ showKey a, b)) $
        M.toList cache

show' :: Parser Command
show' = fmap Command $ Show'
    <$> (optional . strArgument)
        ( metavar "STEP"
       <> help "The name of the step" ) 
    <*> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> metavar "DB_PATH" )

showFlow :: Binary env
         => MVar (M.HashMap Key T.Text)
         -> DataStore
         -> env
         -> SciFlow env
         -> AsyncA (ExceptT () IO) () ()
showFlow cache store env sciflow = eval (AsyncA . runFlow') $ _flow sciflow
  where
    runFlow' (Step job) = runAsyncA $ eval ( \(Action _) ->
        AsyncA $ \ !i -> do
            let key = mkKey i $ _job_name job
            lift (queryStatus store key) >>= \case
                Complete dat -> do
                    let res = decode dat
                    lift $ modifyMVar_ cache $ \dict -> return $ if M.member key dict
                        then dict
                        else M.insert key (T.pack $ ppShow res) dict
                    return res
                _ -> throwError ()
        ) $ _job_action job
    runFlow' (UStep fun) = \i -> lift $ runReaderT (fun i) env