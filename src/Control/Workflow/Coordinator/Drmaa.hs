{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE LambdaCase    #-}

module Control.Workflow.Coordinator.Drmaa
    ( module Control.Workflow.Coordinator
    , DrmaaConfig(..)
    , Drmaa
    , withDrmaa
    ) where

import Control.Monad.Catch (MonadMask)
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

data DrmaaController = DrmaaController 
    { _workers :: M.HashMap ProcessId Worker }

data DrmaaConfig = DrmaaConfig
    { _queue_size :: Int
    , _cmd :: (FilePath, [String])
    , _address :: String
    , _port :: Int
    -- , _job_parameters :: M.HashMap T.Text JobParas
    }

data Drmaa = Drmaa 
    { _drmaa_controller :: TMVar DrmaaController
    , _drmaa_config :: DrmaaConfig }

withDrmaa :: (MonadMask m, MonadIO m) => DrmaaConfig -> (Drmaa -> m a) -> m a
withDrmaa config f = D.withSession $ do
    c <- liftIO $ newTMVarIO $ DrmaaController M.empty
    f $ Drmaa c config

-- | Spawn a new worker
spawnWorker :: DrmaaConfig -> IO ()
spawnWorker config = do
    _ <- D.runJob exe args D.defaultJobAttributes
        {D._env = []}
    return ()
    where
    (exe, args) = _cmd config

workerStatus (Drmaa control _) host = _worker_status .
    M.lookupDefault (error "worker not found") host . _workers <$>
            readTMVar control

setWorkerStatus (Drmaa control _) host status = do
    c <- takeTMVar control
    let c' = c { _workers = M.adjust
            (\x -> x {_worker_status = status}) host $ _workers c }
    putTMVar control c'

instance Coordinator Drmaa where
    type Config Drmaa = DrmaaConfig
    
    queueSize (Drmaa _ config) = _queue_size config

    addToQueue (Drmaa control _) worker = do
        c <- takeTMVar control
        let c' = c { _workers = M.insert (_worker_id worker) worker $ _workers c}
        putTMVar control c'

    getWorkers coord@(Drmaa control _) = M.elems . _workers <$> readTMVar control

    reserve coord@(Drmaa control config) = tryReserve >>= \case
        Left res -> do
            when (isJust res) $ liftIO $ do
                spawnWorker config
                atomically $ putTMVar control $ fromJust res
            liftIO (threadDelay 1000000) >> reserve coord
        Right nd -> return nd
      where
        tryReserve = liftIO $ atomically $ do
            workers <- getWorkers coord
            case filter ((==Idle) . _worker_status) workers of
                (w:_) -> do
                    setWorkerStatus coord (_worker_id w) Working
                    return $ Right $ _worker_id w
                [] -> if length workers < queueSize coord
                    then Left . Just <$> takeTMVar control 
                    else return $ Left Nothing

    release coord worker = liftIO $ atomically $ setWorkerStatus coord worker Idle

    remove coord@(Drmaa control config) worker = do
        send worker Shutdown
        liftIO $ atomically $ do
            c <- takeTMVar control
            let c' = c { _workers = M.delete worker $ _workers c }
            putTMVar control c'

    masterAddr = _address
    masterPort = _port