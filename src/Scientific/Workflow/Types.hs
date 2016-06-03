{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE UndecidableInstances #-}

module Scientific.Workflow.Types
    ( WorkflowDB(..)
    , Workflow(..)
    , Workflows
    , PID
    , ProcState(..)
    , WorkflowState(..)
    , db
    , procStatus
    , Processor
    , RunOpt(..)
    , RunOptSetter
    , defaultRunOpt
    , dbPath
    , Serializable(..)
    , Attribute(..)
    , AttributeSetter
    , defaultAttribute
    , label
    , note
    ) where

import           Control.Exception          (SomeException)
import           Control.Lens               (makeLenses)
import           Control.Monad.State
import           Control.Monad.Trans.Except (ExceptT)
import qualified Data.ByteString            as B
import           Data.Maybe                 (fromJust)
import qualified Data.Text                  as T
import           Data.Yaml                  (FromJSON, ToJSON, decode, encode)
import           Database.SQLite.Simple     (Connection)
import Control.Concurrent.MVar
import qualified Data.Map as M

class Serializable a where
    serialize :: a -> B.ByteString
    deserialize :: B.ByteString -> a

instance (FromJSON a, ToJSON a) => Serializable a where
    serialize = encode
    deserialize = fromJust . decode

-- | An abstract type representing the database used to store states of workflow
data WorkflowDB  = WorkflowDB Connection

-- | The id of a node
type PID = T.Text

-- | The state of a computation node
data ProcState = Success
               | Fail SomeException
               | Scheduled

data WorkflowState = WorkflowState
    { _db         :: WorkflowDB
    , _procStatus :: M.Map PID (MVar ProcState)
    }

makeLenses ''WorkflowState

type Processor a b = a -> StateT WorkflowState (ExceptT (PID, SomeException) IO) b

-- | An Workflow is a DAG without loop and branches
data Workflow where
    Workflow :: (Processor () o) -> Workflow

-- | A list of workflows and ids of all nodes
type Workflows = ([PID], [Workflow])


data RunOpt = RunOpt
    { _dbPath :: FilePath
    }

makeLenses ''RunOpt

defaultRunOpt :: RunOpt
defaultRunOpt = RunOpt
    { _dbPath = "sciflow.db" }

type RunOptSetter = State RunOpt ()


-- | Node attribute
data Attribute = Attribute
    { _label :: T.Text  -- ^ short description
    , _note  :: T.Text   -- ^ long description
    }

makeLenses ''Attribute

defaultAttribute :: Attribute
defaultAttribute = Attribute
    { _label = ""
    , _note = ""
    }

type AttributeSetter = State Attribute ()
