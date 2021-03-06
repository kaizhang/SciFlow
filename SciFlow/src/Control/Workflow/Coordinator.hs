{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase          #-}
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
import Control.Monad.IO.Class (MonadIO)
import Control.Distributed.Process
import GHC.Conc (STM)
import Data.Proxy (Proxy(..))

import Control.Workflow.Types

-- | Coordinator manages a pool of workers.
class Coordinator coordinator where
    -- Configuration

    -- | Configuration
    type Config coordinator = config | config -> coordinator
    setQueueSize :: Int -> Config coordinator -> Config coordinator

    -- Master/server-side process

    -- | Initialize Coordinator on the server.
    withCoordinator :: (MonadMask m, MonadIO m)
                    => Config coordinator -> (coordinator -> m a) -> m a
    -- | Server initiation process
    initiate :: coordinator -> Process ()
    -- | Server shutdown process
    shutdown :: coordinator -> Process ()

    -- Worker/client-side process

    startClient :: Proxy coordinator -> NodeId -> FunctionTable -> IO ()

    -- Operational functions

    -- | Return all workers currently in the pool.
    getWorkers :: coordinator -> STM [Worker]
    -- | Reserve a free worker. This function should block
    -- until a worker is reserved.
    reserve :: coordinator -> Maybe Resource -> Process ProcessId
    -- | Set a worker free but keep it alive so that it can be assigned other jobs.
    freeWorker :: MonadIO m => coordinator -> ProcessId -> m ()
    setWorkerError :: MonadIO m => coordinator -> String -> ProcessId -> m ()

-- | A worker.
data Worker = Worker
    { _worker_id :: ProcessId
    , _worker_status :: WorkerStatus
    , _worker_config :: Maybe Resource
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