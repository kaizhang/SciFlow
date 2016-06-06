{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE GADTs #-}

module Scientific.Workflow.Types
    ( WorkflowDB(..)
    , Workflow(..)
    , Closure(..)
    , PID
    , NodeResult(..)
    , ProcState
    , WorkflowState(..)
    , db
    , procStatus
    , procParaControl
    , remote
    , Processor
    , RunOpt(..)
    , DBData(..)
    , Attribute(..)
    , AttributeSetter
    , defaultAttribute
    , label
    , note
    ) where

import qualified Data.Serialize as S
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

class DBData a where
    serialize :: a -> B.ByteString
    deserialize :: B.ByteString -> a
    showYaml :: a -> B.ByteString
    readYaml :: B.ByteString -> a

instance (FromJSON a, ToJSON a, S.Serialize a) => DBData a where
    serialize = S.encode
    deserialize = fromEither . S.decode
      where
        fromEither (Right x) = x
        fromEither _ = error "decode failed"
    showYaml = encode
    readYaml = fromJust . decode

-- | An abstract type representing the database used to store states of workflow
newtype WorkflowDB  = WorkflowDB Connection

-- | The id of a node
type PID = T.Text

-- | The result of a computation node
data NodeResult = Success
                | Fail SomeException
                | Scheduled

data WorkflowState = WorkflowState
    { _db          :: WorkflowDB
    , _procStatus  :: M.Map PID (MVar NodeResult)
    , _procParaControl :: MVar () -- ^ concurrency controller
    , _remote :: Bool
    }

makeLenses ''WorkflowState

type ProcState b = StateT WorkflowState (ExceptT (PID, SomeException) IO) b
type Processor a b = a -> ProcState b


data Closure where
    Closure :: (DBData a, DBData b) => (a -> IO b) -> Closure

-- | A Workflow is a DAG
data Workflow = Workflow [PID] (M.Map String Closure) (Processor () ())

data RunOpt = RunOpt
    { database :: FilePath
    , nThread :: Int      -- ^ number of concurrent processes
    , runOnRemote :: Bool
    }

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
