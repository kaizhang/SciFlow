module Scientific.Workflow
    ( runWorkflow
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Types
    ) where

import           Control.Concurrent          (forkIO)
import           Control.Concurrent.MVar
import           Control.Exception           (bracket, displayException)
import           Control.Monad.State
import           Control.Monad.Trans.Except
import qualified Data.Map                    as M
import qualified Data.Set                    as S
import Data.Yaml (decodeFile)

import           Scientific.Workflow.Builder
import           Scientific.Workflow.DB
import           Scientific.Workflow.Types
import           Scientific.Workflow.Utils
import           Text.Printf                 (printf)

runWorkflow :: Workflow -> RunOpt -> IO ()
runWorkflow (Workflow pids wf) opts = bracket (openDB $ database opts) closeDB $ \db -> do
    ks <- S.fromList <$> getKeys db
    pidStateMap <- flip M.traverseWithKey pids $ \pid attr -> case runMode opts of
        Normal -> do
            v <- if pid `S.member` ks then newMVar Success else newMVar Scheduled
            return (v, attr)
        ExecSingle i input output -> do
            v <- if pid == i then newMVar (EXE input output) else newMVar Skip
            return (v, attr)
        ReadSingle i -> do
            v <- if pid == i then newMVar Read else newMVar Skip
            return (v, attr)
        WriteSingle i input -> do
            v <- if pid == i then newMVar (Replace input) else newMVar Skip
            return (v, attr)

    para <- newEmptyMVar
    _ <- forkIO $ replicateM_ (nThread opts) $ putMVar para ()

    env <- case configuration opts of
        Nothing -> return M.empty
        Just fl -> do
            r <- decodeFile fl
            case r of
                Nothing -> error "fail to parse configuration file"
                Just x -> return x

    let initState = WorkflowState db pidStateMap para (runOnRemote opts) env

    result <- runExceptT $ evalStateT (wf ()) initState
    case result of
        Right _ -> return ()
        Left (pid, ex) -> errorMsg $ printf "\"%s\" failed. The error was: %s."
            pid (displayException ex)
