{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE Rank2Types            #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}

module Scientific.Workflow.Types
    ( Workflow(..)
    , PID
    , NodeState(..)
    , SpecialMode(..)
    , ProcState
    , WorkflowState(..)
    , database
    , procStatus
    , procParaControl
    , remote
    , Processor
    , RunMode(..)
    , RunOpt(..)
    , defaultRunOpt
    , Parallel(..)
    , WorkflowConfig
    ) where

import           Control.Concurrent.Async.Lifted            (concurrently)
import           Control.Concurrent.MVar                    (MVar)
import           Control.Exception                          (SomeException)
import           Control.Lens                               (makeLenses)
import           Control.Monad.Reader (ReaderT)
import           Control.Monad.Trans.Except                 (ExceptT)
import           Data.Graph.Inductive.Graph                 (labEdges, labNodes,
                                                             mkGraph)
import           Data.Graph.Inductive.PatriciaTree          (Gr)
import qualified Data.Map                                   as M
import qualified Data.Serialize                             as S
import           Data.Serialize.Text                        ()
import qualified Data.Text                                  as T
import qualified Language.Haskell.TH.Lift                   as T

import           Scientific.Workflow.Internal.Builder.Types (Attribute)
import           Scientific.Workflow.Internal.DB            (WorkflowDB (..))

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

data WorkflowState = WorkflowState
    { _database        :: WorkflowDB
    , _procStatus      :: M.Map PID (MVar NodeState, Attribute)
    , _procParaControl :: MVar () -- ^ Concurrency controller
    , _remote          :: Bool    -- ^ Global remote switch
    }

makeLenses ''WorkflowState

type ProcState config = ReaderT WorkflowState (
    ExceptT (PID, SomeException) (WorkflowConfig config) )
type WorkflowConfig config = ReaderT config IO
type Processor config a b = a -> (ProcState config) b

-- | A Workflow is a stateful function
data Workflow config = Workflow
    { _worflow_dag       :: Gr PID Int
    , _worflow_pidToAttr :: M.Map T.Text Attribute
    , _workflow          :: Processor config () ()
    }

-- | Options
data RunOpt = RunOpt
    { dbFile        :: FilePath
    , nThread       :: Int      -- ^ number of concurrent processes
    , runOnRemote   :: Bool
    , runMode       :: RunMode
    , configuration :: [FilePath]
    , selected      :: Maybe [PID]  -- ^ Should run only selected nodes
    }

defaultRunOpt :: RunOpt
defaultRunOpt = RunOpt
    { dbFile = "sciflow.db"
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
