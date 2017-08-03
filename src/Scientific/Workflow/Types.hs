{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE TypeFamilies #-}

module Scientific.Workflow.Types
    ( WorkflowDB(..)
    , Workflow(..)
    , PID
    , NodeState(..)
    , SpecialMode(..)
    , ProcState
    , WorkflowState(..)
    , db
    , procStatus
    , procParaControl
    , remote
    , config
    , getConfig
    , Processor
    , RunMode(..)
    , RunOpt(..)
    , defaultRunOpt
    , DBData(..)
    , Parallel(..)
    ) where

import           Control.Concurrent.Async.Lifted            (concurrently)
import           Control.Concurrent.MVar                    (MVar)
import           Control.Exception                          (SomeException)
import           Control.Lens                               (at, makeLenses,
                                                             (^.))
import           Control.Monad.IO.Class                     (liftIO)
import           Control.Monad.State                        (State, StateT, get)
import           Control.Monad.Trans.Except                 (ExceptT)
import           Data.Aeson.Types
import qualified Data.ByteString                            as B
import           Data.Graph.Inductive.Graph                 (labEdges, labNodes,
                                                             mkGraph)
import           Data.Graph.Inductive.PatriciaTree          (Gr)
import qualified Data.Map                                   as M
import           Data.Maybe                                 (fromJust,
                                                             fromMaybe)
import qualified Data.Serialize                             as S
import           Data.Serialize.Text                        ()
import qualified Data.Text                                  as T
import           Data.Yaml                                  (FromJSON (..),
                                                             ToJSON (..),
                                                             decode, encode)
import           Database.SQLite.Simple                     (Connection)
import           GHC.Generics                               (Generic)
import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift                   as T

import           Scientific.Workflow.Internal.Builder.Types (Attribute)

class DataStore s where
    openStore :: FilePath -> IO s
    closeStore :: s -> IO ()
    writeData :: DBData r => PID -> r -> s -> IO ()
    readData :: DBData r => PID -> s -> IO r

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
        fromEither _         = error "decode failed"
    showYaml = encode
    readYaml = fromJust . decode


-- | An abstract type representing the database used to store states of workflow
newtype WorkflowDB  = WorkflowDB Connection

-- | The id of a node
type PID = T.Text

-- | The result of a computation node
data NodeState = Success                -- ^ The node has been executed
               | Fail SomeException     -- ^ The node failed to finish
               | Scheduled              -- ^ The node will be executed
               | Special SpecialMode    -- ^ Indicate the workflow is currently
                                        -- running under special mode

data SpecialMode = Skip        -- ^ The node will not be executed
                 | FetchData   -- ^ Simply read the saved data from database
                 | WriteData FilePath  -- ^ Read the result from the input file
                                     -- and save it to database.
                 | EXE FilePath FilePath  -- ^ Read input from the input file and
                                        -- save results to the output file. This is
                                        -- used in remote mode.

data WorkflowState config = WorkflowState
    { _db              :: WorkflowDB
    , _procStatus      :: M.Map PID (MVar NodeState, Attribute)
    , _procParaControl :: MVar () -- ^ Concurrency controller
    , _remote          :: Bool    -- ^ Global remote switch
    , _config          :: config  -- ^ Workflow configuration. This
                                  -- is used to store environmental
                                  -- variables.
    }

makeLenses ''WorkflowState

type ProcState config = StateT (WorkflowState config) (ExceptT (PID, SomeException) IO)
type Processor config a b = a -> (ProcState config) b

getConfig :: (ProcState config) config
getConfig = (^.config) <$> get
{-# INLINE getConfig #-}

-- | A Workflow is a stateful function
data Workflow config = Workflow
    { _worflow_dag       :: Gr PID Int
    , _worflow_pidToAttr :: M.Map T.Text Attribute
    , _workflow          :: Processor config () ()
    }

-- | Options
data RunOpt = RunOpt
    { database      :: FilePath
    , nThread       :: Int      -- ^ number of concurrent processes
    , runOnRemote   :: Bool
    , runMode       :: RunMode
    , configuration :: [FilePath]
    , selected      :: Maybe [PID]  -- ^ Should run only selected nodes
    }

defaultRunOpt :: RunOpt
defaultRunOpt = RunOpt
    { database = "sciflow.db"
    , nThread  = 1
    , runOnRemote = False
    , runMode = Master
    , configuration = []
    , selected = Nothing
    }

data RunMode = Master                       -- ^ Run as the master process
             | Slave PID FilePath FilePath  -- ^ Run as a slave process
             | Review PID                   -- ^ Review the info stored in a node
             | Replace PID FilePath         -- ^ Replace the info stored in a node

instance (T.Lift a, T.Lift b) => T.Lift (Gr a b) where
  lift gr = [| uncurry mkGraph $(T.lift (labNodes gr, labEdges gr)) |]

-- | Auxiliary type for concurrency support.
newtype Parallel config r = Parallel { runParallel :: (ProcState config) r}

instance Functor (Parallel config) where
    fmap f (Parallel a) = Parallel $ f <$> a

instance Applicative (Parallel config) where
    pure = Parallel . pure
    Parallel fs <*> Parallel as = Parallel $
        (\(f, a) -> f a) <$> concurrently fs as

instance S.Serialize (Gr (PID, Attribute) Int)
