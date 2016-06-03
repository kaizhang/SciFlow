{-# LANGUAGE CPP #-}
module Scientific.Workflow
    ( runWorkflow
    , getWorkflowState
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Types
    ) where

import Control.Concurrent.Async (mapConcurrently)
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
    mapFn (f initState) wfs >> return ()
  where
    opts = execState optSetter defaultRunOpt
    mapFn | parallel opts = mapConcurrently
          | otherwise = mapM
    f initState (Workflow wf) = do
        result <- runExceptT $ evalStateT (wf ()) initState
        case result of
            Right _ -> return ()
            Left (pid, ex) -> error' $ printf "\"%s\" failed. The error was: %s"
                pid (displayException ex)
