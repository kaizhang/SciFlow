{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase    #-}

module Control.Workflow.Coordinator.Local
    ( LocalConfig(..)
    , Local
    ) where

import           Control.Monad.IO.Class                      (liftIO)
import Control.Distributed.Process
import Control.Concurrent.STM
import Control.Concurrent (threadDelay)

import Control.Workflow.Coordinator

data LocalConfig = LocalConfig
    { _queue_size :: Int }

type WorkerCounter = TMVar Int

data Local = Local WorkerCounter LocalConfig

instance Coordinator Local where
    type Config Local = LocalConfig

    withCoordinator config f =
        (Local <$> liftIO (newTMVarIO 0) <*> return config) >>= f

    initiate _ = return ()
    shutdown _ = return ()
    startClient _ _ _ = return ()
    getWorkers _ = return []

    reserve (Local counter config) _ = liftIO tryReserve >> getSelfPid
      where
        tryReserve = do
            n <- atomically $ takeTMVar counter
            if n < _queue_size config
                then atomically $ putTMVar counter $ n + 1
                else do
                    atomically $ putTMVar counter n
                    threadDelay 1000000
                    tryReserve
   
    freeWorker (Local counter _) _ = liftIO $ atomically $ do
        n <- takeTMVar counter
        putTMVar counter $ n - 1