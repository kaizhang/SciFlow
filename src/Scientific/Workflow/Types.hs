{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE TemplateHaskell #-}
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
import qualified Language.Haskell.TH.Syntax        as TH

import           Scientific.Workflow.Serialization (Serializable (..))
import           Scientific.Workflow.Utils         (fileExist)

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

type ID = String

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
    overwrite <- reader _overwrite
    let file = dir1 ++ "/" ++ dir2 ++ "/" ++ l
    exist <- lift $ fileExist file
    if exist && not overwrite
       then do c <- lift $ B.readFile file
               return $ deserialize c
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

--------------------------------------------------------------------------------
-- Workflow
--------------------------------------------------------------------------------

data Workflows = Workflows WorkflowConfig [Workflow]

data Workflow where
    Workflow :: IOProcessor () b -> Workflow

data WorkflowConfig = WorkflowConfig
    { _baseDir :: !FilePath
    , _logDir :: !FilePath
    , _overwrite :: !Bool
    , _buildMode :: !Mode
    } deriving (Show)

data Mode = All
          | Select [ID] deriving (Show)

instance TH.Lift WorkflowConfig where
    lift (WorkflowConfig a b c d) = [| WorkflowConfig a b c d |]

instance TH.Lift Mode where
    lift All = [| All |]
    lift (Select xs) = [| Select xs |]

instance Default WorkflowConfig where
    def = WorkflowConfig
        { _baseDir = "./"
        , _logDir = "wfCache/"
        , _overwrite = False
        , _buildMode = All
        }

-- data WorkFlowState
data WorkflowState = WorkflowState
    { _nodeStatus :: M.HashMap ID Bool }
