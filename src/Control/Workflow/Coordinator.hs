{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeFamilyDependencies #-}

module Control.Workflow.Coordinator
    ( Signal(..)
    , Worker(..)
    , WorkerStatus(..)
    , Coordinator(..)
    ) where

import Data.Binary (Binary)
import GHC.Generics (Generic)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Distributed.Process (Process, ProcessId, expect)
import GHC.Conc (STM, atomically)

class Coordinator coordinator where
    -- | Configuration
    type Config coordinator = c | c -> coordinator

    -- | Get the size of the queue.
    queueSize :: coordinator -> Int

    addToQueue :: coordinator -> Worker -> STM ()

    getWorkers :: coordinator -> STM [Worker]

    -- | Reserve a free worker.
    reserve :: MonadIO m => coordinator -> m ProcessId

    -- | Release a worker
    release :: MonadIO m => coordinator -> ProcessId -> m ()

    remove :: coordinator -> ProcessId -> Process ()

    masterAddr :: Config coordinator -> String
    masterPort :: Config coordinator -> Int

-- | A worker.
data Worker = Worker
    { _worker_id :: ProcessId
    , _worker_status :: WorkerStatus
    } deriving (Generic, Show)

instance Binary Worker

-- | The status of a worker.
data WorkerStatus = Idle
                  | Working
                  | ErrorExit String
                  deriving (Eq, Generic, Show)

instance Binary WorkerStatus

data Signal = Shutdown deriving (Generic)

instance Binary Signal

