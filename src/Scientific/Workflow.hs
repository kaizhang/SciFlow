module Scientific.Workflow
    ( runWorkflow
    , getWorkflowState
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Types
    ) where

import           Control.Monad.State
import Control.Monad.Trans.Maybe

import           Scientific.Workflow.Builder
import           Scientific.Workflow.Types

runWorkflow :: [Workflow] -> State RunOpt () -> IO ()
runWorkflow wfs setOpt = do
    config <- getWorkflowState $ _dbPath opt
    foldM_ f config wfs
  where
    opt = execState setOpt defaultRunOpt
    f config (Workflow wf) = do
        result <- runMaybeT $ runStateT (wf ()) config
        case result of
            Just (_, config') -> return config'
            _ -> return config
