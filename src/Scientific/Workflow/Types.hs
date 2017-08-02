{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE UndecidableInstances #-}

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
    , getConfigMaybe
    , getConfig'
    , getConfigMaybe'
    , Processor
    , RunMode(..)
    , RunOpt(..)
    , defaultRunOpt
    , DBData(..)
    , Parallel(..)
    , ProcFunction(..)
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

data WorkflowState = WorkflowState
    { _db              :: WorkflowDB
    , _procStatus      :: M.Map PID (MVar NodeState, Attribute)
    , _procParaControl :: MVar () -- ^ Concurrency controller
    , _remote          :: Bool    -- ^ Global remote switch
    , _config          :: M.Map T.Text T.Text    -- ^ Workflow configuration. This
                                                 -- is used to store environmental
                                                 -- variables.
    }

makeLenses ''WorkflowState

type ProcState b = StateT WorkflowState (ExceptT (PID, SomeException) IO) b
type Processor a b = a -> ProcState b

getConfigMaybe :: T.Text -> ProcState (Maybe T.Text)
getConfigMaybe key = do
    st <- get
    return $ (st^.config) ^.at key

getConfig :: T.Text -> ProcState T.Text
getConfig x = fmap (fromMaybe errMsg) $ getConfigMaybe x
  where
    errMsg = error $ "The Key " ++ show x ++ " doesn't exist!"

getConfig' :: T.Text -> ProcState String
getConfig' = fmap T.unpack . getConfig

getConfigMaybe' :: T.Text -> ProcState (Maybe String)
getConfigMaybe' = (fmap.fmap) T.unpack . getConfigMaybe

-- | A Workflow is a stateful function
data Workflow = Workflow (Gr PID Int) (M.Map T.Text Attribute)
                         (Processor () ())

-- | Options
data RunOpt = RunOpt
    { database      :: FilePath
    , nThread       :: Int      -- ^ number of concurrent processes
    , runOnRemote   :: Bool
    , runMode       :: RunMode
    , configuration :: Maybe FilePath
    , selected      :: Maybe [PID]  -- ^ Should run only selected nodes
    }

defaultRunOpt :: RunOpt
defaultRunOpt = RunOpt
    { database = "sciflow.db"
    , nThread  = 1
    , runOnRemote = False
    , runMode = Master
    , configuration = Nothing
    , selected = Nothing
    }

data RunMode = Master                       -- ^ Run as the master process
             | Slave PID FilePath FilePath  -- ^ Run as a slave process
             | Review PID                   -- ^ Review the info stored in a node
             | Replace PID FilePath         -- ^ Replace the info stored in a node


T.deriveLift ''M.Map
T.deriveLift ''Attribute

instance T.Lift T.Text where
  lift t = [| T.pack $(T.lift $ T.unpack t) |]

instance (T.Lift a, T.Lift b) => T.Lift (Gr a b) where
  lift gr = [| uncurry mkGraph $(T.lift (labNodes gr, labEdges gr)) |]


-- | Auxiliary type for concurrency support.
newtype Parallel a = Parallel { runParallel :: ProcState a}

instance Functor Parallel where
    fmap f (Parallel a) = Parallel $ f <$> a

instance Applicative Parallel where
    pure = Parallel . pure
    Parallel fs <*> Parallel as = Parallel $
        (\(f, a) -> f a) <$> concurrently fs as

instance S.Serialize (Gr (PID, Attribute) Int)

class ProcFunction m where
    liftProcFunction :: (a -> m b) -> (a -> ProcState b)

instance ProcFunction IO where
    liftProcFunction fn = liftIO . fn

instance ProcFunction (StateT WorkflowState (ExceptT (PID, SomeException) IO)) where
    liftProcFunction = id
