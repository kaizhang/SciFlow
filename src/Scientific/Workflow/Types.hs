{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}

module Scientific.Workflow.Types
    ( WorkflowDB(..)
    , Workflow(..)
    , DynFunction(..)
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

    , Parallel(..)

    -- * Builder types
    , Node
    , Edge
    , EdgeOrd
    , Builder
    ) where

import           Control.Concurrent.Async.Lifted   (concurrently)
import           Control.Concurrent.MVar           (MVar)
import           Control.Exception                 (SomeException)
import           Control.Lens                      (makeLenses)
import           Control.Monad.State               (State, StateT)
import           Control.Monad.Trans.Except        (ExceptT)
import qualified Data.ByteString                   as B
import           Data.Graph.Inductive.Graph        (labEdges, labNodes, mkGraph)
import           Data.Graph.Inductive.PatriciaTree (Gr)
import           Data.List.Split                   (chunksOf)
import qualified Data.Map                          as M
import           Data.Maybe                        (fromJust)
import qualified Data.Serialize                    as S
import qualified Data.Text                         as T
import           Data.Yaml                         (FromJSON, ToJSON, decode,
                                                    encode)
import           Database.SQLite.Simple            (Connection)
import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift          as T

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

-- | 'BatchData' represents inputs that can be divided into batches and processed
-- in parallel, i.e. list.
class BatchData a b where
    batchFunction :: (a -> IO b) -> Int -> (a -> [a], [b] -> b)

instance (IsList a b ~ flag, BatchData' flag a b) => BatchData a b where
    batchFunction = batchFunction' (undefined :: flag)

-- | 'DBData' type class is used for data serialization.
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
    { _label          :: T.Text      -- ^ Short description
    , _note           :: T.Text      -- ^ Long description
    , _batch          :: Int         -- ^ Batch size. If > 0, inputs will be divided
                                     -- into batches.
    , _submitToRemote :: Maybe Bool  -- ^ Overwrite the global option
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
    { _db              :: WorkflowDB
    , _procStatus      :: M.Map PID (MVar NodeResult, Attribute)
    , _procParaControl :: MVar () -- ^ concurrency controller
    , _remote          :: Bool
    }

makeLenses ''WorkflowState

type ProcState b = StateT WorkflowState (ExceptT (PID, SomeException) IO) b
type Processor a b = a -> ProcState b

-- | Functions with dynamic types.
data DynFunction where
    DynFunction :: (DBData a, DBData b) => (a -> IO b) -> DynFunction

-- | A Workflow is a stateful function
data Workflow = Workflow (M.Map T.Text Attribute)
                         (M.Map String DynFunction)
                         (Processor () ())

-- | Options
data RunOpt = RunOpt
    { database    :: FilePath
    , nThread     :: Int      -- ^ number of concurrent processes
    , runOnRemote :: Bool
    }


-- | Auxiliary type for concurrency support.
newtype Parallel a = Parallel { runParallel :: ProcState a}

instance Functor Parallel where
    fmap f (Parallel a) = Parallel $ f <$> a

instance Applicative Parallel where
    pure = Parallel . pure
    Parallel fs <*> Parallel as = Parallel $
        (\(f, a) -> f a) <$> concurrently fs as


T.deriveLift ''M.Map
T.deriveLift ''Attribute

instance T.Lift T.Text where
  lift t = [| T.pack $(T.lift $ T.unpack t) |]

instance T.Lift (Gr (PID, Attribute) Int) where
  lift gr = [| uncurry mkGraph $(T.lift (labNodes gr, labEdges gr)) |]


-- | The order of incoming edges of a node
type EdgeOrd = Int

-- | A computation node
type Node = (PID, (ExpQ, Attribute))

-- | Links between computational nodes
type Edge = (PID, PID, EdgeOrd)

type Builder = State ([Node], [Edge])
