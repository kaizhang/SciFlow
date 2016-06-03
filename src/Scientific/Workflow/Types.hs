{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE UndecidableInstances #-}

module Scientific.Workflow.Types
    ( WorkflowDB(..)
    , Workflow(..)
    , PID
    , ProcState(..)
    , WorkflowState(..)
    , db
    , procStatus
    , Processor
    , RunOpt(..)
    , defaultRunOpt
    , dbPath
    , Serializable(..)
    , Attribute(..)
    , defaultAttribute
    , label
    , note
    , def
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
import qualified Data.HashTable.IO as M

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
               | Scheduled
               | Fail SomeException

data WorkflowState = WorkflowState
    { _db         :: WorkflowDB
    , _procStatus :: M.CuckooHashTable PID (MVar ProcState)
    }

makeLenses ''WorkflowState

type Processor a b = a -> StateT WorkflowState (ExceptT (PID, SomeException) IO) b

data Workflow where
    Workflow :: (Processor () o) -> Workflow

data RunOpt = RunOpt
    { _dbPath :: FilePath
    }

makeLenses ''RunOpt

defaultRunOpt :: RunOpt
defaultRunOpt = RunOpt
    { _dbPath = "sciflow.db" }

data Attribute = Attribute
    { _label :: T.Text  -- ^ short description
    , _note  :: T.Text   -- ^ long description
    }

defaultAttribute :: Attribute
defaultAttribute = Attribute
    { _label = ""
    , _note = ""
    }

makeLenses ''Attribute

def :: State a ()
def = return ()
