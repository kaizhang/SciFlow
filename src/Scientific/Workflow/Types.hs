{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE CPP #-}
module Scientific.Workflow.Types where

import           Control.Applicative
import           Control.Arrow                     (Arrow (..), Kleisli (..),
                                                    first, second)
import qualified Control.Category                  as C
import           Control.Monad.Reader              (MonadTrans, ReaderT, lift,
                                                    reader, (>=>))
import qualified Data.ByteString                   as B
import           Data.Default.Class
import qualified Data.HashMap.Strict               as M
import qualified Language.Haskell.TH.Lift          as L
import qualified Language.Haskell.TH.Syntax        as TH
import           Debug.Trace

import           Scientific.Workflow.Serialization (Serializable (..))

--------------------------------------------------------------------------------
-- Workflow
--------------------------------------------------------------------------------

type ID = String

instance TH.Lift (M.HashMap ID Bool) where
    lift x = [| M.fromList $(TH.lift $ M.toList x) |]

data Mode = All
          | Select [ID] deriving (Show)

$(L.deriveLift ''Mode)

-- data WorkFlowState
data WorkflowState = WorkflowState
    { _nodeStatus :: M.HashMap ID Bool } deriving (Show)

$(L.deriveLift ''WorkflowState)

finished :: WorkflowState -> [ID]
finished = fst . unzip . filter snd . M.toList . _nodeStatus
{-# INLINE finished #-}

data WorkflowConfig = WorkflowConfig
    { _baseDir :: !FilePath
    , _logDir :: !FilePath
    , _overwrite :: !Bool
    , _buildMode :: !Mode
    , _state :: !WorkflowState
    } deriving (Show)

$(L.deriveLift ''WorkflowConfig)

instance Default WorkflowConfig where
    def = WorkflowConfig
        { _baseDir = "./"
        , _logDir = "wfCache/"
        , _overwrite = False
        , _buildMode = All
        , _state = WorkflowState M.empty
        }

data Workflows = Workflows WorkflowConfig [Workflow]

data Workflow where
    Workflow :: IOProcessor () b -> Workflow

--------------------------------------------------------------------------------
-- Arrow
--------------------------------------------------------------------------------

-- | labeled Arrow
newtype Processor m a b = Processor { runProcessor :: a -> m b }

instance Monad m => C.Category (Processor m) where
    id = Processor return
    (Processor f) . (Processor g) = Processor $ g >=> f

instance Monad m => Arrow (Processor m) where
    arr f = Processor (return . f)
    first (Processor f) = Processor (\ ~(b,d) -> f b >>= \c -> return (c,d))
    second (Processor f) = Processor (\ ~(d,b) -> f b >>= \c -> return (d,c))

-- | Label is a pair of side effects
type Label m l o = (l -> m (Maybe o), l -> o -> m ())

-- | Turn a Kleisli arrow into a labeled arrow
label :: (MonadTrans t, Monad m, Monad (t m))
      => Label (t m) l b
      -> l
      -> Kleisli m a b
      -> Processor (t m) a b
label (pre, suc) l (Kleisli f) = Processor $ \x -> do
    d <- pre l
    case d of
        Nothing -> do
            o <- lift $ f x
            suc l o
            return o
        Just v -> return v
{-# INLINE label #-}


class Arrow a => Actor a b c where
    arrIO :: a b c -> Kleisli IO b c

instance Actor (->) a b where
    arrIO = arr

instance Actor (Kleisli IO) a b where
    arrIO = id

proc :: Actor ar a b => Serializable b => ID -> ar a b -> IOProcessor a b
proc l ar = label (recover, save) l $ arrIO ar

source :: Serializable o => ID -> o -> Source o
source l x = proc l $ const x

nullSource :: Source o
nullSource = label (const $ return $ Just undefined, undefined) "" $ arr $ const undefined

emptySource :: Source ()
emptySource = label (const $ return $ Just undefined, undefined) "" $ arr $ const ()

recover :: Serializable a => ID -> ReaderT WorkflowConfig IO (Maybe a)
recover l = do
    dir1 <- reader _baseDir
    dir2 <- reader _logDir
    alreadyRun <- reader (M.lookupDefault False l . _nodeStatus . _state)

    #ifdef DEBUG
    traceM $ l ++ " already finished: "  ++ show alreadyRun
    #endif

    let file = dir1 ++ "/" ++ dir2 ++ "/" ++ l
    if alreadyRun
       then lift $ fmap deserialize $ B.readFile file  -- recover
       else return Nothing

save :: Serializable a => ID -> a -> ReaderT WorkflowConfig IO ()
save l x = do
    dir1 <- reader _baseDir
    dir2 <- reader _logDir
    lift $ B.writeFile (dir1 ++ "/" ++ dir2  ++ "/" ++ l) $ serialize x


type IOProcessor = Processor (ReaderT WorkflowConfig IO)

-- | Source produce an output without inputs
type Source = IOProcessor ()

instance Functor Source where
    fmap f (Processor g) = Processor $ fmap f . g

instance Applicative Source where
    pure = Processor . const . return
    Processor f <*> Processor g = Processor $ \x -> do
        a <- f x
        b <- g x
        return $ a b
