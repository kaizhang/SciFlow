{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase    #-}

module Control.Workflow.Coordinator.Drmaa
    ( Drmaa
    , DrmaaConfig(..)
    , getDefaultDrmaaConfig
    , MainOpts(..)
    , defaultMainOpts
    , mainWith
    ) where

import           Control.Monad.IO.Class                      (liftIO)
import qualified Data.HashMap.Strict as M
import qualified DRMAA as D
import qualified Control.Funflow.ContentStore                as CS
import Data.Binary (Binary)
import Control.Distributed.Process
import Control.Concurrent.STM
import Control.Concurrent (threadDelay)
import System.Environment (getArgs, getExecutablePath)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Path (parseAbsDir)
import System.Directory (makeAbsolute)

import Control.Workflow.Coordinator
import Control.Workflow.Types
import Control.Workflow.Interpreter.Exec

data DrmaaConfig = DrmaaConfig
    { _queue_size :: Int
    , _cmd :: (FilePath, [String])
    , _address :: String
    , _port :: Int
    -- , _job_parameters :: M.HashMap T.Text JobParas
    }

getDefaultDrmaaConfig :: IO DrmaaConfig
getDefaultDrmaaConfig = do
    exePath <- getExecutablePath
    return $ DrmaaConfig
        { _queue_size = 5
        , _cmd = (exePath, ["--slave"])
        , _address = "192.168.0.1"
        , _port = 8888 }

type WorkerPool = TMVar (M.HashMap ProcessId Worker)

data Drmaa = Drmaa WorkerPool DrmaaConfig

instance Coordinator Drmaa where
    type Config Drmaa = DrmaaConfig

    withCoordinator config f = D.withSession $ do
        c <- liftIO $ newTMVarIO M.empty
        f $ Drmaa c config

    getWorkers (Drmaa pool _) = M.elems <$> readTMVar pool

    addToPool (Drmaa pool _) worker =
        (M.insert (_worker_id worker) worker <$> takeTMVar pool) >>= 
            putTMVar pool

    reserve coord@(Drmaa pool config) = tryReserve >>= \case
        Right nd -> return nd
        Left Nothing -> liftIO (threadDelay 1000000) >> reserve coord
        Left (Just w) -> do
            liftIO $ spawnWorker config
            worker <- expect :: Process Worker
            let w' = M.insert (_worker_id worker) worker{_worker_status = Working} w
            liftIO $ do
                putStrLn $ "Found a new worker: " ++ show (_worker_id worker)
                atomically $ putTMVar pool w'
            return $ _worker_id worker
      where
        tryReserve = liftIO $ atomically $ do
            workers <- getWorkers coord
            case filter ((==Idle) . _worker_status) workers of
                (w:_) -> do
                    setWorkerStatus coord (_worker_id w) Working
                    return $ Right $ _worker_id w
                [] -> if length workers < _queue_size config
                    then Left . Just <$> takeTMVar pool
                    else return $ Left Nothing
   
    release coord worker = liftIO $ atomically $ setWorkerStatus coord worker Idle

    remove (Drmaa pool _) worker = do
        send worker Shutdown
        liftIO $ atomically $
            (M.delete worker <$> takeTMVar pool) >>= putTMVar pool

-- | Spawn a new worker
spawnWorker :: DrmaaConfig -> IO ()
spawnWorker config = do
    _ <- D.runJob exe args D.defaultJobAttributes {D._env = []}
    return ()
  where
    (exe, args) = _cmd config

setWorkerStatus (Drmaa pool _) host status =
    (M.adjust (\x -> x {_worker_status = status}) host <$> takeTMVar pool) >>=
        putTMVar pool

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
    exePath <- getExecutablePath
    let host = _master_addr
        port = _master_port
        config = DrmaaConfig
            { _queue_size = _n_workers
            , _cmd = (exePath, ["--slave"])
            , _address = host
            , _port = port }
    getArgs >>= \case
        ["--slave"] -> initClient host port $ _function_table wf
        _ -> withCoordinator config $ \drmaa -> do
            Right transport <- createTransport (defaultTCPAddr host (show port))
                defaultTCPParameters
            dir <- makeAbsolute _store_path >>= parseAbsDir
            CS.withStore dir $ \store -> 
                runSciFlow drmaa transport store env wf

