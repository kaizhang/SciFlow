{-# LANGUAGE CPP #-}
module Scientific.Workflow
    ( runWorkflow
    , getWorkflowState
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Types
    ) where

import           Control.Exception           (displayException, bracket)
import           Control.Monad.State
import           Control.Monad.Trans.Except
import qualified Data.Set as S
import qualified Data.Map as M
import Control.Concurrent.MVar

import           Scientific.Workflow.Builder
import           Scientific.Workflow.DB
import           Scientific.Workflow.Types
import           Scientific.Workflow.Utils
import Text.Printf (printf)

runWorkflow :: Workflows -> RunOptSetter -> IO ()
runWorkflow (pids, wfs) optSetter = bracket (openDB $ _dbPath opts) closeDB $ \db -> do
    ks <- S.fromList <$> getKeys db
    pidStateMap <- fmap M.fromList $ forM pids $ \pid -> do
        v <- if pid `S.member` ks
                then newMVar Success
                else newMVar Scheduled
        return (pid, v)
    let initState = WorkflowState db pidStateMap
#ifdef DEBUG
    debug $ printf "Executing %d workflow(s)" (length wfs)
#endif
    foldM_ f initState wfs
  where
    opts = execState optSetter defaultRunOpt
    f initState (Workflow wf) = do
        result <- runExceptT $ runStateT (wf ()) initState
        case result of
            Right (_, finalState) -> return finalState
            Left (pid, ex) -> do
                error' $ printf "\"%s\" failed. The error was: %s"
                    pid (displayException ex)
                return initState
