{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
module Control.Workflow.Types
    ( SciFlow(..)
    , FunctionTable(..)
    , JobConfig(..)
    , Job(..)
    , Node(..)
    , Workflow(..)
    , Builder
    , RemoteException
    , Flow(..)
    , step
    , ustep
    ) where

import qualified Data.HashMap.Strict as M
import Data.Binary (Binary)
import Control.Monad.Reader
import Control.Exception.Safe (Exception)
import qualified Data.Text as T
import           Language.Haskell.TH
import Control.Monad.State.Lazy (State)
import Control.Funflow.ContentHashable (ContentHash)
import Control.Arrow.Free (Choice, effect)
import qualified Data.ByteString.Lazy as B
import Control.Distributed.Process.Serializable (SerializableDict)
import Control.Distributed.Process (Process, RemoteTable, Closure, Static)

data SciFlow env = SciFlow
    { _flow :: Choice (Flow env) () ()
    , _function_table :: FunctionTable }

data FunctionTable = FunctionTable
    { _table :: (T.Text, B.ByteString, B.ByteString) -> Closure (Process (Maybe B.ByteString))
    , _dict :: Static (SerializableDict (Maybe B.ByteString))
    , _rtable :: RemoteTable }

data Flow env i o where
  Step :: (Binary i, Binary o) => Job env i o -> Flow env i o
  UStep :: (i -> ReaderT env IO o) -> Flow env i o   -- ^ `UStep` will not be cached and run every time.

step :: (Binary i, Binary o) => Job env i o -> Choice (Flow env) i o
step job = effect $ Step job

ustep :: (i -> ReaderT env IO o) -> Choice (Flow env) i o
ustep = effect . UStep 

-- | Configuration of jobs.
data JobConfig = JobConfig
    { _job_config_cpu :: Maybe Int
    , _job_config_memory :: Maybe Int    -- ^ memory in MB
    } deriving (Show)

-- | A wrapper for individual step.
data Job env i o = Job
    { _job_name   :: T.Text
    , _job_config :: JobConfig
    , _job_action :: i -> ReaderT env IO o
    , _job_cache :: i -> ContentHash }

-- | A computation node.
data Node = Node
    { _node_function :: Name  -- ^ a function with type: a -> Process b
    , _node_parallel :: Bool }

-- | Workflow declaration, containing a map of nodes and their parental processes.
data Workflow = Workflow
    { _nodes :: M.HashMap T.Text Node
    , _parents :: M.HashMap T.Text [T.Text] }

instance Semigroup Workflow where
    x <> y = Workflow (_nodes x <> _nodes y) (_parents x <> _parents y)

type Builder = State Workflow

newtype RemoteException = RemoteException String deriving (Show)

instance Exception RemoteException