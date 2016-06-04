{-# LANGUAGE CPP #-}
module Scientific.Workflow
    ( runWorkflow
    , getWorkflowState
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Types
    ) where

import           Control.Concurrent.MVar
import           Control.Concurrent (forkIO)
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

runWorkflow :: Workflows -> RunOptSetter -> IO ()
runWorkflow (pids, wfs) optSetter = bracket (openDB $ _dbPath opts) closeDB $ \db -> do
    ks <- S.fromList <$> getKeys db
    pidStateMap <- fmap M.fromList $ forM pids $ \pid -> do
        v <- if pid `S.member` ks
                then newMVar Success
                else newMVar Scheduled
        return (pid, v)
    para <- newEmptyMVar
    forkIO $ replicateM_ 2 $ putMVar para ()
    let initState = WorkflowState db pidStateMap para
#ifdef DEBUG
    debug $ printf "Executing %d workflow(s)" (length wfs)
#endif
    mapM_ (f initState) wfs
  where
    opts = execState optSetter defaultRunOpt
    f initState (Workflow wf) = do
        result <- runExceptT $ evalStateT (wf ()) initState
        case result of
            Right _ -> return ()
            Left (pid, ex) -> error' $ printf "\"%s\" failed. The error was: %s"
                pid (displayException ex)
