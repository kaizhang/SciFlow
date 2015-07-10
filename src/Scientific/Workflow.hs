module Scientific.Workflow
    ( module Scientific.Workflow.Types
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Builder.TH
    , module Scientific.Workflow.Serialization.Yaml
    , runWorkflow
    , mapA
    ) where

import           Control.Arrow                          (Kleisli (..))
import           Control.Monad.Reader                   (forM_, runReaderT)
import qualified Data.Text                              as T
import           Shelly                                 (fromText, mkdir_p,
                                                         shelly)

import           Scientific.Workflow.Builder
import           Scientific.Workflow.Builder.TH
import           Scientific.Workflow.Serialization.Yaml
import           Scientific.Workflow.Types

runWorkflow :: [Workflow] -> WorkflowConfig -> IO ()
runWorkflow wfs config = do
    shelly $ mkdir_p $ fromText $ T.pack dir
    forM_ wfs $ \(Workflow wf) -> do
        _ <- runReaderT (runProcessor wf ()) config
        return ()
  where
    dir = _baseDir config ++ "/" ++ _logDir config

mapA :: Monad m => Kleisli m a b -> Kleisli m [a] [b]
mapA (Kleisli f) = Kleisli $ mapM f
