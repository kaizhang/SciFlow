{-# LANGUAGE FlexibleInstances #-}
module Scientific.Workflow.Types where

import Data.Default.Class (Default(..))
import qualified Data.HashMap.Strict as M
import Control.Exception.Safe (SomeException)
import Control.Funflow
import qualified Data.Text as T
import           Language.Haskell.TH

-- | Configuration of jobs.
data JobConfig = JobConfig
    { _job_config_cpu :: Maybe Int
    , _job_config_memory :: Maybe Int    -- ^ memory in MB
    } deriving (Show)

-- | A wrapper for individual step.
data Job m a b = Job
    { _job_name   :: T.Text
    , _job_config :: JobConfig
    , _job_action :: a -> m b
    }

-- | Core workflow type.
type SciFlow m = Flow (Job m) SomeException

-- | A computation node.
data Node = Node
    { _node_function :: ExpQ
    }

-- | Workflow declaration, containing a map of nodes and their parental processes.
data Workflow = Workflow
    { _nodes :: M.HashMap T.Text Node
    , _parents :: M.HashMap T.Text [T.Text]
    }

instance Default Workflow where
    def = Workflow M.empty M.empty

-- | Objects that can be converted to ExpQ.
class ToExpQ a where
    toExpQ :: a -> ExpQ

instance ToExpQ Name where
    toExpQ = varE

instance ToExpQ (Q Exp) where
    toExpQ = id

