{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeFamilyDependencies #-}
--------------------------------------------------------------------------------
-- |
-- Module      :  Control.Workflow.Coordinator
-- Copyright   :  (c) 2019 Kai Zhang
-- License     :  MIT
-- Maintainer  :  kai@kzhang.org
-- Stability   :  experimental
-- Portability :  portable
--
-- Coordinator needs to be able to discover new workers and send commands
-- to workers. The implementation of Coordinator thus contains server and
-- client parts. Server-side codes are executed by `withCoordinator` and 
-- client-side codes are executed by `initClient`.
--
--------------------------------------------------------------------------------

module Control.Workflow.Coordinator
    ( Signal(..)
    , Worker(..)
    , WorkerStatus(..)
    , Coordinator(..)
    ) where

import Data.Binary (Binary)
import Control.Monad.Catch (MonadMask)
import GHC.Generics (Generic)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Distributed.Process (Process, ProcessId, expect)
import GHC.Conc (STM, atomically)

class Coordinator coordinator where
    -- | Configuration
    type Config coordinator = config | config -> coordinator

    -- | Initialize Coordinator on the server.
    withCoordinator :: (MonadMask m, MonadIO m)
                    => Config coordinator -> (coordinator -> m a) -> m a

    -- | Get all workers.
    getWorkers :: coordinator -> STM [Worker]

    -- | Add a worker to the worker pool.
    addToPool :: coordinator -> Worker -> STM ()

    -- | Reserve a free worker. This function should block
    -- until a worker is reserved.
    reserve :: coordinator -> Process ProcessId

    -- | Set a worker free.
    release :: MonadIO m => coordinator -> ProcessId -> m ()

    remove :: coordinator -> ProcessId -> Process ()

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

