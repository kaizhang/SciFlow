module Scientific.Workflow
    ( module Scientific.Workflow.Types
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Builder.TH
    , module Scientific.Workflow.Serialization.Yaml
    , runWorkflow
    , mapA
    ) where

import           Control.Arrow                          (Kleisli (..))
import           Control.Monad.State                    (forM_, evalStateT)
import qualified Data.Text                              as T
import           Shelly                                 (fromText, mkdir_p,
                                                         shelly)

import           Scientific.Workflow.Builder
import           Scientific.Workflow.Builder.TH
import           Scientific.Workflow.Serialization.Yaml
import           Scientific.Workflow.Types

runWorkflow :: Workflows -> IO ()
runWorkflow (Workflows config ps) = do
    shelly $ mkdir_p $ fromText $ T.pack dir
    forM_ ps $ \(Workflow p) -> do
        _ <- evalStateT (runProcessor p ()) config
        return ()
  where
    dir = _baseDir config ++ "/" ++ _logDir config

mapA :: Monad m => Kleisli m a b -> Kleisli m [a] [b]
mapA (Kleisli f) = Kleisli $ mapM f
