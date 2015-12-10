module Scientific.Workflow
    ( runWorkflow
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Types
    ) where

import           Control.Monad.State
import qualified Data.Map                    as M

import           Scientific.Workflow.Builder
import           Scientific.Workflow.DB
import           Scientific.Workflow.Types

runWorkflow :: [Workflow] -> State RunOpt () -> IO ()
runWorkflow wfs setOpt = do
    db <- openDB $ _dbPath opt
    ks <- getKeys db
    pSt <- mapM (flip isFinished db) ks
    let pSts = M.fromList $ zipWith (\k s ->
                 if s then (k, Finished) else (k, Scheduled)) ks pSt
        config = WorkflowState db pSts
    foldM_ f config wfs
  where
    opt = execState setOpt defaultRunOpt
    f config (Workflow wf) = do
        (_, config') <- runStateT (wf ()) config
        return config'

defaultRunOpt :: RunOpt
defaultRunOpt = RunOpt
    { _dbPath = "wfDB" }
