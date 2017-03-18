{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE UndecidableInstances  #-}

module Scientific.Workflow.Types
    ( WorkflowDB(..)
    , Workflow(..)
    , PID
    , NodeState(..)
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
    , ContextData(..)
    , Attribute(..)
    , AttributeSetter
    , defaultAttribute
    , label
    , note
    , batch
    , submitToRemote
    , stateful
    , remoteParam

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
import           Control.Lens                      (at, makeLenses, (^.))
import           Control.Monad.State               (State, StateT, get)
import           Control.Monad.Trans.Except        (ExceptT)
import qualified Data.ByteString                   as B
import           Data.Graph.Inductive.Graph        (labEdges, labNodes, mkGraph)
import           Data.Graph.Inductive.PatriciaTree (Gr)
import qualified Data.Map                          as M
import           Data.Maybe                        (fromJust, fromMaybe)
import qualified Data.Serialize                    as S
import qualified Data.Text                         as T
import           Data.Yaml                         (FromJSON(..), ToJSON(..), decode,
                                                    encode,)
import Data.Aeson.Types
import           Database.SQLite.Simple            (Connection)
import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift          as T
import           GHC.Generics          (Generic)

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
        fromEither _ = error "decode failed"
    showYaml = encode
    readYaml = fromJust . decode

data ContextData context dat = ContextData
    { _context :: context
    , _data :: dat
    } deriving (Generic)

instance (FromJSON c, FromJSON d) => FromJSON (ContextData c d) where
    parseJSON = genericParseJSON defaultOptions

instance (ToJSON c, ToJSON d) => ToJSON (ContextData c d) where
    toEncoding = genericToEncoding defaultOptions

instance (S.Serialize c, S.Serialize d) => S.Serialize (ContextData c d)


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
    , _stateful       :: Bool        -- ^ Whether the node function has access
                                     -- to internal states
    , _remoteParam    :: String
    }

makeLenses ''Attribute

defaultAttribute :: Attribute
defaultAttribute = Attribute
    { _label = ""
    , _note = ""
    , _batch = -1
    , _submitToRemote = Nothing
    , _stateful = False
    , _remoteParam = ""
    }

type AttributeSetter = State Attribute ()

-- | The result of a computation node
data NodeState = Success                -- ^ The node has been executed
               | Fail SomeException     -- ^ The node failed to finish
               | Scheduled              -- ^ The node will be executed
               | Skip                   -- ^ The node will not be executed
               | Get                    -- ^ Simply read the saved data from database
               | Put FilePath           -- ^ Read the result from the input file
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


-- | The order of incoming edges of a node
type EdgeOrd = Int

-- | A computation node
type Node = (PID, (ExpQ, Attribute))

-- | Links between computational nodes
type Edge = (PID, PID, EdgeOrd)

type Builder = State ([Node], [Edge])


-- | Auxiliary type for concurrency support.
newtype Parallel a = Parallel { runParallel :: ProcState a}

instance Functor Parallel where
    fmap f (Parallel a) = Parallel $ f <$> a

instance Applicative Parallel where
    pure = Parallel . pure
    Parallel fs <*> Parallel as = Parallel $
        (\(f, a) -> f a) <$> concurrently fs as
