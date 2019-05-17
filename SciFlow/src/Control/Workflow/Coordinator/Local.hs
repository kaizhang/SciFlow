{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase    #-}

module Control.Workflow.Coordinator.Local
    ( LocalConfig(..)
    , Local
    , MainOpts(..)
    , defaultMainOpts
    , mainWith
    ) where

import           Control.Monad.IO.Class                      (liftIO)
import Control.Distributed.Process
import Control.Concurrent.STM
import Control.Concurrent (threadDelay)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Data.Binary (Binary)
import Path (parseAbsDir)
import qualified Control.Funflow.ContentStore                as CS
import System.Directory (makeAbsolute)

import Control.Workflow.Coordinator
import Control.Workflow.Types
import Control.Workflow.Interpreter.Exec

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
    startClient _ _ = return ()
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

data MainOpts = MainOpts
    { _store_path :: FilePath
    , _master_addr :: String
    , _master_port :: Int
    , _n_workers :: Int
    }

defaultMainOpts :: MainOpts
defaultMainOpts = MainOpts
    { _store_path = "sciflow_db"
    , _master_addr = "192.168.0.1"
    , _master_port = 8888
    , _n_workers = 5
    }

mainWith :: Binary env
         => MainOpts
         -> env
         -> SciFlow env
         -> IO ()
mainWith MainOpts{..} env wf = do
    let host = _master_addr
        port = show _master_port
        config = LocalConfig { _queue_size = _n_workers }
    withCoordinator config $ \coord -> do
        Right transport <- createTransport (defaultTCPAddr host port)
            defaultTCPParameters
        dir <- makeAbsolute _store_path >>= parseAbsDir
        CS.withStore dir $ \store -> 
            runSciFlow coord transport store env wf

