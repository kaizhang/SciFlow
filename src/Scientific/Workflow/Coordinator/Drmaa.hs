{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE LambdaCase          #-}

module Scientific.Workflow.Coordinator.Drmaa
    ( module Scientific.Workflow.Coordinator
    , DrmaaConfig(..)
    , Drmaa
    , withDrmaa
    ) where

import Control.Monad.Catch (bracket)
import System.Environment (getEnv)
import           Control.Funflow.ContentHashable
import System.Random (randomIO)
import qualified Data.HashMap.Strict as M
import Data.Binary (encode, decode)
import Data.List (intercalate)
import           Control.Monad.IO.Class                      (liftIO)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Control.Concurrent.MVar
import qualified DRMAA as D
import Control.Concurrent.STM
import Control.Concurrent (threadDelay, forkFinally, forkIO)
import Control.Monad (forever, void)
import Network.Socket (Socket)
import qualified Network.Socket as S
import Network.Socket.ByteString (recv, sendAll)

import Scientific.Workflow.Coordinator


data DrmaaController = DrmaaController 
    { _jobs :: M.HashMap ContentHash JobStatus
    , _workers :: M.HashMap HostId Worker }

data DrmaaConfig = DrmaaConfig
    { _queue_size :: Int
    , _cmd :: (FilePath, [String])
    , _address :: (Int, Int, Int, Int)
    , _port :: Int
    -- , _job_parameters :: M.HashMap T.Text JobParas
    }

data Drmaa = Drmaa 
    { _drmaa_controller :: TMVar DrmaaController
    , _drmaa_config :: DrmaaConfig }

withDrmaa :: DrmaaConfig -> (Drmaa -> IO a) -> IO a
withDrmaa config f = D.withSession $ Drmaa <$>
    newTMVarIO (DrmaaController M.empty M.empty) <*> return config >>= f

instance Coordinator Drmaa where
    data Connection Drmaa = DrmaaConn Socket
    type Config Drmaa = DrmaaConfig
    
    initialize coord@(Drmaa _ config) = do
        _ <- forkIO $ do
            addr <- resolve $ show $ _port config
            bracket (open addr) S.close loop
        return ()
      where
        resolve port = do
            let hints = S.defaultHints { S.addrFlags = [S.AI_PASSIVE]
                                     , S.addrSocketType = S.Stream }
            addr:_ <- S.getAddrInfo (Just hints) Nothing (Just port)
            return addr
        open addr = do
            sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) $
                S.addrProtocol addr
            S.setSocketOption sock S.ReuseAddr 1
            S.bind sock $ S.addrAddress addr
            -- If the prefork technique is not used,
            -- set CloseOnExec for the security reasons.
            let fd = S.fdSocket sock
            S.setCloseOnExecIfNeeded fd
            S.listen sock 10
            return sock
        loop sock = forever $ do
            (conn, peer) <- S.accept sock
            putStrLn $ "Connection from " ++ show peer
            void $ forkFinally (talk conn) (\_ -> S.close conn)
        talk conn = fromBytes <$> recv conn 4096 >>= \case
            Nothing -> error "decode content hash failed"
            Just chash -> do
                r <- atomically (jobStatus coord chash) >>= \case
                    InProgress i -> return $ Just i
                    _ -> return Nothing
                sendAll conn $ toStrict $ encode r
                talk conn

    queueSize (Drmaa _ config) = _queue_size config

    currWorkers coord@(Drmaa control _) =
        M.elems . _workers <$> readTMVar control

    spawnWorker coord@(Drmaa control config) = liftIO $ do
        host <- randHost
        _ <- forkFinally (start host) $ \case
            Left ex -> atomically $ setWorkerStatus coord host $
                ErrorExit $ show ex
            Right _ -> return ()
        return host
      where
        (exe, args) = _cmd config
        start host = D.runAndWait exe args D.defaultJobAttributes
            {D._env = [("SCIFLOW_HOST_ID", show host)]}

    workerStatus (Drmaa control _) host = _worker_status .
        M.lookupDefault (error "worker not found") host . _workers <$>
            readTMVar control

    setWorkerStatus (Drmaa control _) host status = do
        c <- takeTMVar control
        let c' = c { _workers = M.adjust
                (\x -> x {_worker_status = status}) host $ _workers c }
        putTMVar control c'

    submitJob coord@(Drmaa control _) chash = trySubmit >>= \case
        Right host -> return host
        Left Nothing -> liftIO (threadDelay 1000000) >> submitJob coord chash
        Left (Just c) -> do
            host <- spawnWorker coord
            let worker = Worker host Idle
            liftIO $ atomically $ do
                putTMVar control $ c
                    { _workers = M.insert host worker $ _workers c}
                assignJob coord chash host
            return host
      where
        trySubmit = liftIO $ atomically $ do
            workers <- currWorkers coord
            case filter ((==Idle) . _worker_status) workers of
                (w:_) -> assignJob coord chash (_worker_host_name w) >>= \case
                    Left ex -> error ex
                    Right () -> return $ Right $ _worker_host_name w
                _ -> if length workers < queueSize coord
                    then Left . Just <$> takeTMVar control 
                    else return $ Left Nothing
        
    jobStatus (Drmaa control _) chash = M.lookupDefault Unscheduled chash .
        _jobs <$> readTMVar control

    setJobStatus (Drmaa control _) chash status = do
        c <- takeTMVar control
        let c' = c { _jobs = M.insert chash status $ _jobs c }
        putTMVar control c'

    withConnection config f = bracket connect close f
      where
        connect = liftIO $ do
            let hints = S.defaultHints { S.addrSocketType = S.Stream }
                (a,b,c,d) = _address config
                server = intercalate "." $ map show [a,b,c,d]

            addr:_ <- S.getAddrInfo (Just hints) (Just server) (Just $ show $ _port config)
            sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) $
                S.addrProtocol addr
            S.connect sock $ S.addrAddress addr
            return $ DrmaaConn sock
        close (DrmaaConn sock) = liftIO $ S.close sock

    getJobHost (DrmaaConn sock) chash = do
        sendAll sock $ toBytes chash
        decode . fromStrict <$> recv sock 4096
    getClientId _ = read <$> getEnv "SCIFLOW_HOST_ID"

randHost :: IO Int
randHost = randomIO
