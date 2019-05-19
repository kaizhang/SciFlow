{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE GADTs #-}
module Control.Workflow.Types
    ( SciFlow(..)
    , FunctionTable(..)
    , ResourceConfig(..)
    , Resource(..)
    , Job(..)
    , Action(..)
    , Flow(..)
    , step
    , ustep
    ) where

import Data.Binary (Binary)
import Control.Monad.Reader
import GHC.Generics (Generic)
import qualified Data.Text as T
import Control.Funflow.ContentHashable (ContentHash)
import Control.Arrow.Free (Free, Choice, effect)
import qualified Data.ByteString.Lazy as B
import qualified Data.HashMap.Strict as M
import Control.Distributed.Process.Serializable (SerializableDict)
import Control.Distributed.Process (Process, RemoteTable, Closure, Static)
import Language.Haskell.TH.Lift (deriveLift)

-- | The core type, containing the workflow represented as a free arrow and 
-- a function table for remote execution.
data SciFlow env = SciFlow
    { _flow :: Free (Flow env) () ()
    , _function_table :: FunctionTable }

-- | The function table that can be sent to remote.
data FunctionTable = FunctionTable
    { _table :: (T.Text, B.ByteString, B.ByteString)
             -> Closure (Process (Maybe B.ByteString))
    , _dict :: Static (SerializableDict (Maybe B.ByteString))
    , _rtable :: RemoteTable }

-- | Global job specific resource configuration. This will overwrite any
-- existing configuration.
newtype ResourceConfig = ResourceConfig
    { _resource_config :: M.HashMap T.Text Resource }

-- | The basic component/step of a workflow.
data Job env i o = Job
    { _job_name   :: T.Text   -- ^ The name of the job
    , _job_descr  :: T.Text   -- ^ The description of the job
    , _job_resource :: Maybe Resource   -- ^ The computational resource needed
    , _job_parallel :: Bool    -- ^ Whether to run this step in parallel
    , _job_action :: Choice (Action env) i o }   -- ^ The action to run

data Action env i o where
    Action :: (Binary i, Binary o) =>
        { _unAction :: i -> ReaderT env IO o   -- ^ The function to run
        , _action_cache :: i -> ContentHash    -- ^ Cache function
        } -> Action env i o

-- | Free arrow side effect.
data Flow env i o where
    Step :: (Binary i, Binary o) => Job env i o -> Flow env i o
    UStep :: (i -> ReaderT env IO o) -> Flow env i o   -- ^ `UStep` will not be cached.

step :: (Binary i, Binary o) => Job env i o -> Free (Flow env) i o
step job = effect $ Step job

ustep :: (i -> ReaderT env IO o) -> Free (Flow env) i o
ustep = effect . UStep 

-- | Computational resource
data Resource = Resource
    { _num_cpu :: Maybe Int   -- ^ The number of CPU needed
    , _total_memory :: Maybe Int    -- ^ Memory in GB
    , _submit_queue :: Maybe String -- ^ Job submitting queue
    } deriving (Eq, Generic, Show)

instance Binary Resource

deriveLift 'Resource