module Scientific.Workflow
    ( module Scientific.Workflow.Types
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Builder.TH
    , module Scientific.Workflow.Serialization.Yaml
    , runWorkflow
    , mapA
    ) where

import           Control.Monad.State                    (forM_, execStateT)
import qualified Data.Text                              as T
import Data.List (foldl')
import           Shelly                                 (fromText, mkdir_p,
                                                         shelly)

import           Scientific.Workflow.Builder
import           Scientific.Workflow.Builder.TH
import           Scientific.Workflow.Serialization.Yaml
import           Scientific.Workflow.Types
import           Scientific.Workflow.Utils

runWorkflow :: [Workflow] -> RunOpt -> IO ()
runWorkflow wfs opt = do
    shelly $ mkdir_p $ fromText $ T.pack dir
    st <- do
        db <- mkNodesDB opt
        return $ if _runForce opt
            then case _runMode opt of
                Select xs -> foldl' (\d x -> writeNodeStatus x Unfinished d) db xs
                _ -> db
            else db

    let config = WorkflowConfig (_runDir opt) (_runLogDir opt) st
    forM_ wfs $ \(Workflow wf) -> do
        _ <- execStateT (runProcessor wf ()) config
        return ()
  where
    dir = _runDir opt ++ "/" ++ _runLogDir opt
