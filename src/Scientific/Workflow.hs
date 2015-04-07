module Scientific.Workflow
    ( module Scientific.Workflow.Types
    , module Scientific.Workflow.Builder
    , module Scientific.Workflow.Builder.TH
    , module Scientific.Workflow.Serialization.Yaml
    , runWorkflow
    , mapA
    ) where

import Control.Arrow (Kleisli(..))
import Control.Monad.Reader (runReaderT)
import qualified Data.Text as T
import Shelly (mkdir_p, shelly, fromText)

import Scientific.Workflow.Builder
import Scientific.Workflow.Builder.TH
import Scientific.Workflow.Types
import Scientific.Workflow.Serialization.Yaml

runWorkflow :: SourceProc b -> WorkflowOpt -> IO b
runWorkflow nd opt = do
    shelly $ mkdir_p $ fromText $ T.pack $ _logDir opt
    runReaderT (runProc nd ()) $ Config $ _logDir opt

mapA :: Monad m => Kleisli m a b -> Kleisli m [a] [b]
mapA (Kleisli f) = Kleisli $ mapM f
