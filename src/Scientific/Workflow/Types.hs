{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE CPP #-}
module Scientific.Workflow.Types where

import           Control.Applicative
import           Control.Arrow                     (Arrow (..), Kleisli (..),
                                                    first, second)
import qualified Control.Category                  as C
import Control.Lens (makeLenses, use, (%=))
import           Control.Monad.State               (MonadTrans, StateT, lift,
                                                     (>=>))
import qualified Data.ByteString                   as B
import           Data.Default.Class
import qualified Data.HashMap.Strict               as M
import qualified Data.Text                         as T
import qualified Language.Haskell.TH.Lift          as L
import Shelly (shelly, test_d, lsT, fromText)

import           Scientific.Workflow.Serialization (Serializable (..))

import Debug.Trace

--------------------------------------------------------------------------------
-- Workflow
--------------------------------------------------------------------------------

type ID = String

data NodeState = Finished
               | Unfinished
               | Skip
    deriving (Show)

type NodesDB = M.HashMap ID NodeState

data WorkflowConfig = WorkflowConfig
    { _baseDir :: !FilePath
    , _logDir :: !FilePath
    , _nodeStatus :: !NodesDB
    }

makeLenses ''WorkflowConfig

readNodeStatus :: ID -> NodesDB -> NodeState
readNodeStatus = M.lookupDefault Unfinished
{-# INLINE readNodeStatus #-}

writeNodeStatus :: ID -> NodeState -> NodesDB -> NodesDB
writeNodeStatus = M.insert
{-# INLINE writeNodeStatus #-}

data Mode = All
          | Select [ID]

L.deriveLift ''Mode

data RunOpt = RunOpt
    { _runDir :: !FilePath
    , _runLogDir :: !FilePath
    , _runMode :: !Mode
    , _runForce :: !Bool
    }

L.deriveLift ''RunOpt

instance Default RunOpt where
    def = RunOpt
        { _runDir = "./"
        , _runLogDir = "wfCache/"
        , _runMode = All
        , _runForce = False
        }

mkNodesDB :: RunOpt -> IO NodesDB
mkNodesDB opt = do
    fls <- shelly $ do
        e <- test_d $ fromText $ T.pack dir
        if e then lsT $ fromText $ T.pack dir
             else return []
    return $ M.fromList $
        zip (map (T.unpack . snd . T.breakOnEnd "/") fls) $ repeat Finished
  where
    dir = _runDir opt ++ "/" ++ _runLogDir opt ++ "/"

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
nullSource = label (const $ return $ Just undefined, undefined) ("" :: String) $ arr $ const undefined

recover :: Serializable a => ID -> StateT WorkflowConfig IO (Maybe a)
recover l = do
    st <- readNodeStatus l <$> use nodeStatus

#ifdef DEBUG
    traceM $ "Process node: " ++ l ++ " . Status is: " ++ show st
#endif

    case st of
        Finished -> do
            dir1 <- use baseDir
            dir2 <- use logDir
            let file = dir1 ++ "/" ++ dir2 ++ "/" ++ l
            (Just . deserialize) <$> lift (B.readFile file)
        Unfinished -> return Nothing
        Skip -> return $ Just undefined
{-# INLINE recover #-}

save :: Serializable a => ID -> a -> StateT WorkflowConfig IO ()
save l x = do
    dir1 <- use baseDir
    dir2 <- use logDir
    lift $ B.writeFile (dir1 ++ "/" ++ dir2  ++ "/" ++ l) $ serialize x
    nodeStatus %= writeNodeStatus l Finished

#ifdef DEBUG
    traceM $ "Finish node: " ++ l ++ "\n"
#endif
{-# INLINE save #-}

type IOProcessor = Processor (StateT WorkflowConfig IO)

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
