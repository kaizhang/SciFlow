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

import           Scientific.Workflow.Builder
import           Scientific.Workflow.DB
import           Scientific.Workflow.Types
import           Scientific.Workflow.Utils
import           Text.Printf                 (printf)

runWorkflow :: Workflow -> RunOpt -> IO ()
runWorkflow (Workflow pids _ wf) opts = bracket (openDB $ database opts) closeDB $ \db -> do
    ks <- S.fromList <$> getKeys db
    pidStateMap <- flip M.traverseWithKey pids $ \pid attr -> do
        v <- if pid `S.member` ks then newMVar Success else newMVar Scheduled
        return (v, attr)
    para <- newEmptyMVar
    _ <- forkIO $ replicateM_ (nThread opts) $ putMVar para ()
    let initState = WorkflowState db pidStateMap para $ runOnRemote opts
    result <- runExceptT $ evalStateT (wf ()) initState
    case result of
        Right _ -> return ()
        Left (pid, ex) -> error' $ printf "\"%s\" failed. The error was: %s"
            pid (displayException ex)
