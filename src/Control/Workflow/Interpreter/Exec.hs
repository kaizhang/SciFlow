{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}

module Control.Workflow.Interpreter.Exec (execFlow) where

import           Control.Arrow.Async
import           Control.Arrow.Free                          (eval)
import Control.Monad.Reader
import Control.Monad.Except (ExceptT, runExceptT, throwError)
import           Control.Exception.Safe                      (SomeException,
                                                              bracket,
                                                              handleAny)
import           Control.Funflow hiding (Step)
import           Control.Funflow.ContentHashable
import qualified Control.Funflow.ContentStore                as CS
import           Control.Monad.IO.Class                      (MonadIO, liftIO)
import           Control.Monad.Trans.Class                   (lift)
import Control.Concurrent (threadDelay)
import Control.Distributed.Process (processNodeId, getSelfPid, register, call, Process)
import Network.Transport (Transport)
import Control.Distributed.Process.Node
import Control.Distributed.Process.MonadBaseControl ()
import qualified Data.ByteString.Lazy                             as BS
import GHC.Conc (atomically)
import Data.Binary (Binary(..), encode, decode)
import           Katip
import           Path
import           System.IO                                   (stderr)

import Control.Workflow.Types
import Control.Workflow.Coordinator

-- | Flow interpreter.
execFlow :: forall coordinator env . (Coordinator coordinator, Binary env)
         => coordinator
         -> CS.ContentStore
         -> env
         -> SciFlow env
         -> AsyncA (KatipContextT (ExceptT SomeException Process)) () ()
execFlow coord store env sciflow = eval (AsyncA . runFlow') $ _flow sciflow
  where
    runFlow' (Step w) = lift . runJob coord store (_function_table sciflow) env w
    runFlow' (UStep f) = \x -> 
        lift $ lift $ liftIO $ runReaderT (f x) env

runJob :: (Coordinator coordinator, Binary i, Binary o, Binary env)
       => coordinator
       -> CS.ContentStore
       -> FunctionTable
       -> env
       -> Job env i o
       -> (i -> ExceptT SomeException Process o)
runJob coord store remoteFlow env j@Job{..} input
    | _job_parallel = runAction coord store remoteFlow env j input
    {-
    | otherwise = runner input
  where
    runner i = CS.waitUntilComplete store chash >>= \case
        Just item -> liftIO $ decode <$> BS.readFile (simpleOutPath item)
        Nothing -> handleAny cleanUp $ lift $ do
            fp <- CS.markPending store chash
            -- callLocal (_job_action input) >>= writeStore store chash fp
            pid <- reserve coord
            liftIO $ putStrLn $ "Working: " ++ show chash
            res <- call (_dict remoteFlow) (processNodeId pid) $
                (_table remoteFlow) (_job_name, encode env, encode i)
            case res of
                Nothing -> error "error"
                Just r -> do
                    release coord pid
                    writeStore store chash fp $ decode r
      where
        chash = _action_cache _job_action i
        cleanUp ex = do
            CS.removeFailed store chash
            throwError ex
    simpleOutPath item = toFilePath $ CS.itemPath store item </> [relfile|out|]
    -}

runAction :: (Coordinator coordinator, Binary i, Binary o, Binary env)
       => coordinator
       -> CS.ContentStore
       -> FunctionTable
       -> env
       -> Job env i o
       -> (i -> ExceptT SomeException Process o)
runAction coord store remoteFlow env Job{..} = 
    runAsyncA $ eval (AsyncA . runner) $ _job_action
  where
    runner :: Action env i o -> (i -> ExceptT SomeException Process o)
    runner act i = CS.waitUntilComplete store chash >>= \case
        Just item -> liftIO $ decode <$> BS.readFile (simpleOutPath item)
        {-
        Nothing -> handleAny cleanUp $ lift $ do
            fp <- CS.markPending store chash
            -- callLocal (_job_action input) >>= writeStore store chash fp
            pid <- reserve coord
            liftIO $ putStrLn $ "Working: " ++ show chash
            res <- call (_dict remoteFlow) (processNodeId pid) $
                (_table remoteFlow) (_job_name, encode env, encode [i])
            case res of
                Nothing -> error "error"
                Just r -> do
                    release coord pid
                    writeStore store chash fp $ head $ decode r
                    -}
      where
        chash = _action_cache act i
        cleanUp ex = do
            CS.removeFailed store chash
            throwError ex
    simpleOutPath item = toFilePath $ CS.itemPath store item </> [relfile|out|]


--------------------------------------------------------------------------------
-- Store functions
--------------------------------------------------------------------------------

writeStore :: (Binary o, MonadIO m)
           => CS.ContentStore -> ContentHash -> Path Abs Dir -> o -> m o
writeStore store chash fp res = do
    liftIO $ BS.writeFile (toFilePath $ fp </> [relfile|out|]) $ encode res
    _ <- CS.markComplete store chash
    return res
{-# INLINE writeStore #-}