module Scientific.Workflow
    ( runWorkflow
    , getWorkflowState
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Types
    ) where

import           Control.Monad.State
import Control.Monad.Trans.Except
import Control.Exception (displayException)

import           Scientific.Workflow.Builder
import           Scientific.Workflow.Types
import System.IO

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
            Left ex -> do
                hPutStrLn stderr $ displayException ex
                return config
