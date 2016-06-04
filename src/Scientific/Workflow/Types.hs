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
    , NodeResult(..)
    , ProcState
    , WorkflowState(..)
    , db
    , procStatus
    , procParaControl
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

import           Control.Concurrent.MVar
import           Control.Exception          (SomeException)
import           Control.Lens               (makeLenses)
import           Control.Monad.State
import           Control.Monad.Trans.Except (ExceptT)
import qualified Data.ByteString            as B
import qualified Data.Map                   as M
import           Data.Maybe                 (fromJust)
import qualified Data.Text                  as T
import           Data.Yaml                  (FromJSON, ToJSON, decode, encode)
import           Database.SQLite.Simple     (Connection)

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

-- | The result of a computation node
data NodeResult = Success
                | Fail SomeException
                | Scheduled

data WorkflowState = WorkflowState
    { _db          :: WorkflowDB
    , _procStatus  :: M.Map PID (MVar NodeResult)
    , _procParaControl :: MVar ()   -- ^ concurrency controller
    }

makeLenses ''WorkflowState

type ProcState b = StateT WorkflowState (ExceptT (PID, SomeException) IO) b
type Processor a b = a -> ProcState b

-- | An Workflow is a DAG without loop and branches
data Workflow where
    Workflow :: (Processor () o) -> Workflow

-- | A list of workflows and ids of all nodes
type Workflows = ([PID], [Workflow])


data RunOpt = RunOpt
    { _dbPath  :: FilePath
    , parallel :: Bool
    }

makeLenses ''RunOpt

defaultRunOpt :: RunOpt
defaultRunOpt = RunOpt
    { _dbPath = "sciflow.db"
    , parallel = False
    }

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
