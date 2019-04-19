{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE LambdaCase    #-}

module Control.Workflow.Coordinator.Drmaa
    ( module Control.Workflow.Coordinator
    , DrmaaConfig(..)
    , Drmaa
    ) where

import Control.Monad.IO.Class (MonadIO)
import qualified Data.HashMap.Strict as M
import           Control.Monad.IO.Class                      (liftIO)
import qualified DRMAA as D
import Control.Distributed.Process
import Control.Concurrent.STM
import Control.Concurrent (threadDelay)
import Control.Monad (when)
import Data.Maybe (fromJust, isJust)

import Control.Workflow.Coordinator

data DrmaaConfig = DrmaaConfig
    { _queue_size :: Int
    , _cmd :: (FilePath, [String])
    , _address :: String
    , _port :: Int
    -- , _job_parameters :: M.HashMap T.Text JobParas
    }

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

    remove coord@(Drmaa pool config) worker = do
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

addToQueue :: Drmaa -> Worker -> STM ()
addToQueue (Drmaa pool _) worker =
    (M.insert (_worker_id worker) worker <$> takeTMVar pool) >>= 
        putTMVar pool

workerStatus (Drmaa pool _) host = _worker_status .
    M.lookupDefault (error "worker not found") host <$>
            readTMVar pool

setWorkerStatus (Drmaa pool _) host status =
    (M.adjust (\x -> x {_worker_status = status}) host <$> takeTMVar pool) >>=
        putTMVar pool

