module Scientific.Workflow
    ( module Scientific.Workflow.Types
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Builder.TH
    , module Scientific.Workflow.Serialization.Yaml
    , runWorkflow
    , mapA
    ) where

import           Control.Arrow                          (Kleisli (..))
import           Control.Monad.State                    (forM_, execStateT)
import qualified Data.Text                              as T
import           Shelly                                 (fromText, mkdir_p,
                                                         shelly)

import           Scientific.Workflow.Builder
import           Scientific.Workflow.Builder.TH
import           Scientific.Workflow.Serialization.Yaml
import           Scientific.Workflow.Types

runWorkflow :: [Workflow] -> RunOpt -> IO ()
runWorkflow wfs opt = do
    shelly $ mkdir_p $ fromText $ T.pack dir
    st <- mkNodesDB opt
    let config = WorkflowConfig (_runDir opt) (_runLogDir opt) st
    forM_ wfs $ \(Workflow wf) -> do
        _ <- execStateT (runProcessor wf ()) config
        return ()
  where
    dir = _runDir opt ++ "/" ++ _runLogDir opt

mapA :: Monad m => Kleisli m a b -> Kleisli m [a] [b]
mapA (Kleisli f) = Kleisli $ mapM f
