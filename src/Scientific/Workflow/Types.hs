{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}

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
    , BatchData(..)
    , BatchData'(..)
    , IsList
    , DBData(..)
    , Attribute(..)
    , AttributeSetter
    , defaultAttribute
    , label
    , note
    , batch
    , submitToRemote
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
import Data.List.Split (chunksOf)

data HTrue
data HFalse

type family IsList a b where
    IsList [a] [b] = HTrue
    IsList a b = HFalse

class BatchData' flag a b where
    batchFunction' :: flag -> (a -> IO b) -> Int -> (a -> [a], [b] -> b)

instance BatchData' HTrue [a] [b] where
    batchFunction' _ _ i = (chunksOf i, concat)

instance BatchData' HFalse a b where
    batchFunction' _ _ _ = (return, head)

class BatchData a b where
    batchFunction :: (a -> IO b) -> Int -> (a -> [a], [b] -> b)

instance (IsList a b ~ flag, BatchData' flag a b) => BatchData a b where
    batchFunction = batchFunction' (undefined :: flag)

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

-- | Node attribute
data Attribute = Attribute
    { _label :: T.Text  -- ^ short description
    , _note  :: T.Text   -- ^ long description
    , _batch :: Int
    , _submitToRemote :: Maybe Bool  -- ^ overwrite the global option
    }

makeLenses ''Attribute

defaultAttribute :: Attribute
defaultAttribute = Attribute
    { _label = ""
    , _note = ""
    , _batch = -1
    , _submitToRemote = Nothing
    }

type AttributeSetter = State Attribute ()

-- | The result of a computation node
data NodeResult = Success
                | Fail SomeException
                | Scheduled

data WorkflowState = WorkflowState
    { _db          :: WorkflowDB
    , _procStatus  :: M.Map PID (MVar NodeResult, Attribute)
    , _procParaControl :: MVar () -- ^ concurrency controller
    , _remote :: Bool
    }

makeLenses ''WorkflowState

type ProcState b = StateT WorkflowState (ExceptT (PID, SomeException) IO) b
type Processor a b = a -> ProcState b


data Closure where
    Closure :: (DBData a, DBData b) => (a -> IO b) -> Closure

-- | A Workflow is a DAG
data Workflow = Workflow (M.Map T.Text Attribute)
                         (M.Map String Closure)
                         (Processor () ())

data RunOpt = RunOpt
    { database :: FilePath
    , nThread :: Int      -- ^ number of concurrent processes
    , runOnRemote :: Bool
    }
