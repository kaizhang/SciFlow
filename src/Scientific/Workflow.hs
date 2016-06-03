module Scientific.Workflow
    ( runWorkflow
    , getWorkflowState
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Types
    ) where

import           Control.Exception           (displayException)
import           Control.Monad.State
import           Control.Monad.Trans.Except

import           Scientific.Workflow.Builder
import           Scientific.Workflow.Types
import           Scientific.Workflow.Utils
import Text.Printf (printf)

runWorkflow :: [Workflow] -> State RunOpt () -> IO ()
runWorkflow wfs setOpt = do
    config <- getWorkflowState $ _dbPath opt
    foldM_ f config wfs
  where
    opt = execState setOpt defaultRunOpt
    f config (Workflow wf) = do
        result <- runExceptT $ runStateT (wf ()) config
        case result of
            Right (_, config') -> return config'
            Left (pid, ex) -> do
                error' $ printf "\"%s\" failed. The error was: %s"
                    pid (displayException ex)
                return config
