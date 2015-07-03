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

runWorkflow :: Workflow -> IO ()
runWorkflow (Workflow config ps) = do
    shelly $ mkdir_p $ fromText $ T.pack dir
    forM_ ps $ \p -> do
        _ <- runReaderT (runProcessor p ()) config
        return ()
  where
    dir = _baseDir config ++ "/" ++ _logDir config

--readWorkflowState :: WorkflowOpt -> IO WorkflowState
--readWorkflowState opt = do

mapA :: Monad m => Kleisli m a b -> Kleisli m [a] [b]
mapA (Kleisli f) = Kleisli $ mapM f
