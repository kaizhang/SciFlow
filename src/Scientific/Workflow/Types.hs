{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE UndecidableInstances  #-}

module Scientific.Workflow.Types
    ( WorkflowDB(..)
    , Workflow(..)
    , PID
    , NodeResult(..)
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
    , DBData(..)
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
import           Data.Yaml                         (FromJSON, ToJSON, decode,
                                                    encode)
import           Database.SQLite.Simple            (Connection)
import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift          as T

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
data NodeResult = Success                -- ^ The node has been executed
                | Fail SomeException     -- ^ The node failed to finish
                | Scheduled              -- ^ The node will be executed
                | Skip                   -- ^ The node will not be executed
                | Read                   -- ^ Simply read the saved data from database
                | Replace FilePath         -- ^ Read the result from the input file
                                         -- and save it to database.
                | EXE FilePath FilePath  -- ^ Read input from the input file and
                                         -- save results to the output file. This is
                                         -- used in remote mode.

data WorkflowState = WorkflowState
    { _db              :: WorkflowDB
    , _procStatus      :: M.Map PID (MVar NodeResult, Attribute)
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
data Workflow = Workflow (M.Map T.Text Attribute)
                         (Processor () ())

-- | Options
data RunOpt = RunOpt
    { database      :: FilePath
    , nThread       :: Int      -- ^ number of concurrent processes
    , runOnRemote   :: Bool
    , runMode       :: RunMode
    , configuration :: Maybe FilePath
    }

data RunMode = Normal
             | ExecSingle PID FilePath FilePath
             | ReadSingle PID
             | WriteSingle PID FilePath

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
