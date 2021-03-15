{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveLift #-}
{-# LANGUAGE GADTs #-}
module Control.Workflow.Types
    ( SciFlow(..)
    , NodeLabel(..)
    , FunctionTable(..)
    , ResourceConfig(..)
    , Resource(..)
    , Job(..)
    , Action(..)
    , Flow(..)
    , Env
    , step
    , ustep
    ) where

import Data.Binary (Binary)
import Control.Monad.Reader
import GHC.Generics (Generic)
import qualified Data.Text as T
import Data.Typeable (Typeable)
import Control.Arrow.Free (Free, Choice, effect)
import qualified Data.Graph.Inductive as G
import qualified Data.ByteString.Lazy as B
import qualified Data.HashMap.Strict as M
import Control.Distributed.Process.Serializable (SerializableDict)
import Control.Distributed.Process (Process, RemoteTable, Closure, Static)
import Language.Haskell.TH.Syntax (Lift)

-- | The core type, containing the workflow represented as a free arrow and 
-- a function table for remote execution.
data SciFlow env = SciFlow
    { _flow :: Free (Flow env) () ()
    , _function_table :: FunctionTable
    , _graph :: G.Gr (Maybe NodeLabel) ()}

data NodeLabel = NodeLabel
    { _label :: T.Text
    , _descr :: T.Text
    , _parallel :: Bool
    } deriving (Show, Generic, Lift)

-- | The function table that can be sent to remote.
data FunctionTable = FunctionTable
    { _table :: (T.Text, B.ByteString, B.ByteString)
             -> Closure (Process (Either String B.ByteString))
    , _dict :: Static (SerializableDict (Either String B.ByteString))
    , _rtable :: RemoteTable }

-- | Global job specific resource configuration. This will overwrite any
-- existing configuration.
newtype ResourceConfig = ResourceConfig
    { _resource_config :: M.HashMap T.Text Resource }

-- TODO: This should be implemented using dependent type in future.
-- | The basic component/step of a workflow.
data Job env i o = Job
    { _job_name   :: T.Text   -- ^ The name of the job
    , _job_descr  :: T.Text   -- ^ The description of the job
    , _job_resource :: Maybe Resource   -- ^ The computational resource needed
    , _job_parallel :: Bool    -- ^ Whether to run this step in parallel
    , _job_action :: Choice (Action env) i o }   -- ^ The action to run

type Env env = ReaderT env IO

data Action env i o where
    Action :: (Typeable i, Typeable o, Binary i, Binary o, Show i, Show o) =>
        { _unAction :: i -> Env env o   -- ^ The function to run
        } -> Action env i o

-- | Free arrow side effect.
data Flow env i o where
    Step :: (Binary i, Binary o) => Job env i o -> Flow env i o   -- ^ A cached step
    UStep :: (i -> Env env o) -> Flow env i o   -- ^ An uncached step

step :: (Binary i, Binary o) => Job env i o -> Free (Flow env) i o
step job = effect $ Step job
{-# INLINE step #-}

ustep :: (i -> Env env o) -> Free (Flow env) i o
ustep job = effect $ UStep job
{-# INLINE ustep #-}

-- | Computational resource
data Resource = Resource
    { _num_cpu :: Maybe Int   -- ^ The number of CPU needed
    , _total_memory :: Maybe Int    -- ^ Memory in GB
    , _submit_params :: Maybe String -- ^ Job submitting queue
    } deriving (Eq, Generic, Show, Lift)

instance Binary Resource