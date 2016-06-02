{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

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
    , Attribute
    , defaultAttribute
    , label
    , note
    , def
    ) where

import Control.Lens (makeLenses)
import           Control.Monad.State
import Control.Monad.Trans.Except (ExceptT)
import Control.Exception (SomeException)
import qualified Data.ByteString     as B
import qualified Data.Map            as M
import qualified Data.Text           as T
import Data.Maybe (fromJust)
import Data.Yaml (FromJSON, ToJSON, encode, decode)

class Serializable a where
    serialize :: a -> B.ByteString
    deserialize :: B.ByteString -> a

instance (FromJSON a, ToJSON a) => Serializable a where
    serialize = encode
    deserialize = fromJust . decode

-- | An abstract type representing the database used to store states of workflow
data WorkflowDB  = WorkflowDB FilePath

-- | The id of a node
type PID = T.Text

-- | The state of a computation node
data ProcState = Success
               | Scheduled
               | Fail SomeException

data WorkflowState = WorkflowState
    { _db         :: WorkflowDB
    , _procStatus :: M.Map PID ProcState
    }

makeLenses ''WorkflowState

type Processor a b = a -> StateT WorkflowState (ExceptT SomeException IO) b

data Workflow where
    Workflow :: (Processor () o) -> Workflow

data RunOpt = RunOpt
    { _dbPath :: FilePath
    }

makeLenses ''RunOpt

defaultRunOpt :: RunOpt
defaultRunOpt = RunOpt
    { _dbPath = "wfDB" }

data Attribute = Attribute
    { _label :: T.Text  -- ^ short description
    , _note :: T.Text   -- ^ long description
    }

defaultAttribute :: Attribute
defaultAttribute = Attribute
    { _label = ""
    , _note = ""
    }

makeLenses ''Attribute

def :: State a ()
def = return ()
