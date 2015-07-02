{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
module Scientific.Workflow.Types where

import Control.Applicative
import Control.Arrow (Kleisli(..), Arrow(..), first, second)
import qualified Control.Category as C
import Control.Monad.Reader (ReaderT, lift, reader, (>=>), MonadTrans)
import qualified Data.ByteString as B
import Data.Default.Class
import qualified Data.Text as T
import Shelly (shelly, test_f, fromText)

import Scientific.Workflow.Serialization (Serializable(..))

data Workflow where
    Workflow :: IOProcessor () b -> Workflow

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

type IOProcessor = Processor (ReaderT Config IO)

type Actor = Kleisli IO

actor :: (a -> IO b) -> Actor a b
actor = Kleisli

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

proc :: Serializable b => String -> Kleisli IO a b -> IOProcessor a b
proc = label (recover, save)

source :: Serializable o => String -> o -> Source o
source l x = proc l $ arr $ const x

recover :: Serializable a => String -> ReaderT Config IO (Maybe a)
recover l = do
    dir <- reader _baseDir
    let file = dir ++ l
    exist <- lift $ fileExist file
    if exist
       then do c <- lift $ B.readFile file
               return $ deserialize c
       else return Nothing

save :: Serializable a => String -> a -> ReaderT Config IO ()
save l x = do
    dir <- reader _baseDir
    lift $ B.writeFile (dir++l) $ serialize x

fileExist :: FilePath -> IO Bool
fileExist x = shelly $ test_f $ fromText $ T.pack x

data Config = Config
    { _baseDir :: !FilePath
    }

data WorkflowOpt = WorkflowOpt
    { _logDir :: !FilePath
    }

instance Default WorkflowOpt where
    def = WorkflowOpt
        { _logDir = "wfCache/"
        }
