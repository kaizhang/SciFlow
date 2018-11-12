{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeFamilyDependencies #-}

module Scientific.Workflow.Coordinator
    ( HostId
    , Worker(..)
    , WorkerStatus(..)
    , JobStatus(..)
    , Coordinator(..)
    , runCoordinator
    ) where

import           Control.Arrow.Async
import           Control.Arrow.Free                          (eval)
import Control.Monad.IO.Class (MonadIO)
import           Control.Exception.Safe (MonadMask, SomeException, try, bracket)
import           Control.Funflow hiding (runFlow)
import           Control.Funflow.ContentHashable (ContentHash)
import qualified Control.Funflow.ContentStore                as CS
import           Control.Monad.IO.Class                      (liftIO)
import Control.Monad.Trans.Control (MonadBaseControl)
import GHC.Conc (STM, atomically)
import           Control.Monad.Trans.Class                   (lift)
import qualified Data.ByteString                             as BS
import           Katip
import           Path
import           System.IO                                   (stderr)
import Control.Concurrent (threadDelay)

import Scientific.Workflow.Types

type HostId = Int

data Worker = Worker
    { _worker_host_name :: HostId
    , _worker_status :: WorkerStatus
    }

data WorkerStatus = Idle
                  | Working
                  | ErrorExit String
                  deriving (Eq)

data JobStatus = InProgress HostId
               | Unscheduled
               | Complete
               | Failed String
               deriving (Eq)

class Coordinator coordinator where
    -- | Client-host connection
    data Connection coordinator

    -- | Configuration
    type Config coordinator = c | c -> coordinator

    initialize :: coordinator -> IO ()

    queueSize :: coordinator -> Int

    -- | Get a free worker from the pool. If no worker is free 
    --and the queue is not full, spawn a new worker.
    currWorkers :: coordinator -> STM [Worker]

    spawnWorker :: MonadIO m => coordinator -> m HostId

    workerStatus :: coordinator -> HostId -> STM WorkerStatus

    setWorkerStatus :: coordinator -> HostId -> WorkerStatus -> STM ()

    submitJob :: MonadIO m => coordinator -> ContentHash -> m HostId

    assignJob :: coordinator -> ContentHash -> HostId -> STM (Either String ())
    assignJob coord chash host = workerStatus coord host >>= \case
        Idle -> do
            setWorkerStatus coord host Working 
            setJobStatus coord chash $ InProgress host
            return $ Right ()
        _ -> return $ Left "Worker is not available"
    

    jobStatus :: coordinator -> ContentHash -> STM JobStatus

    setJobStatus :: coordinator -> ContentHash -> JobStatus -> STM ()

    -- Client side functions
    withConnection :: (MonadIO m, MonadMask m)
                   => Config coordinator
                   -> (Connection coordinator -> m a)
                   -> m a
    getJobHost :: Connection coordinator -> ContentHash -> IO (Maybe HostId)
    getClientId :: Connection coordinator -> IO HostId

-- | Coordinate the execution of a flow
runCoordinator :: ( MonadMask m, MonadIO m, MonadBaseControl IO m
                  , Coordinator coordinator)
               => coordinator
               -> CS.ContentStore
               -> SciFlow m a b
               -> a
               -> m (Either SomeException b)
runCoordinator coord store flow input = do
    liftIO $ initialize coord
    handleScribe <- liftIO $ mkHandleScribe ColorIfTerminal stderr InfoS V2
    let mkLogEnv = liftIO $ registerScribe "stderr" handleScribe defaultScribeSettings =<<
            initLogEnv "SciFlow" "production"
    bracket mkLogEnv (liftIO . closeScribes) $ \le -> do
        let initialContext = ()
            initialNamespace = "executeLoop"
        runKatipContextT le initialContext initialNamespace $ try $
            runAsyncA (runCoordinator' coord store flow) input

runCoordinator' :: ( MonadMask m, MonadIO m, MonadBaseControl IO m
                   , Coordinator coordinator)
                => coordinator
                -> CS.ContentStore
                -> SciFlow m a b
                -> AsyncA (KatipContextT m) a b
runCoordinator' coord store = eval $ AsyncA . \case
    Step _ f -> return . f
    StepIO _ f -> liftIO . f
    Wrapped props w -> \input -> case cache props of
        NoCache -> (lift . _job_action w) input
        c -> runStep coord store c input
    _ -> error "Not implemented"
{-# INLINE runCoordinator' #-}

runStep :: (MonadIO m, Coordinator coordinator)
        => coordinator
        -> CS.ContentStore
        -> Cacher i o
        -> i
        -> KatipContextT m o
runStep coord store cache input = checkStore >>= \case
    Just contents -> return contents
    Nothing -> do
        host <- submitJob coord chash
        waitForComplete host
  where
    chash = cacherKey cache confIdent input
    confIdent = 12345
    simpleOutPath item = toFilePath $ CS.itemPath store item </> [relfile|out|]
    waitForComplete host = checkStore >>= \case
        Just contents -> do
            liftIO $ atomically $ do 
                setJobStatus coord chash Complete
                setWorkerStatus coord host Idle
            return contents
        Nothing -> (liftIO $ atomically $ jobStatus coord chash) >>= \case
            InProgress _ -> liftIO (threadDelay 1000000) >> waitForComplete host
            Complete -> undefined
            Unscheduled -> do
                liftIO $ CS.listPending store >>= mapM_ (CS.removeFailed store)
                error "Not scheduled"
            Failed e -> do
                liftIO $ CS.listPending store >>= mapM_ (CS.removeFailed store)
                error e
    checkStore = CS.lookup store chash >>= \case
        CS.Complete item -> do
            bs <- liftIO . BS.readFile $ simpleOutPath item
            return . Just . cacherReadValue cache $ bs
        _ -> return Nothing
{-# INLINE runStep #-}

