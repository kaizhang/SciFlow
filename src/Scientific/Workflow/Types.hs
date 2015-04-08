module Scientific.Workflow.Types where

import qualified Control.Category as C
import Control.Arrow (Kleisli(..), Arrow(..), first, second)
import Control.Monad.Reader (ReaderT, lift, reader, (>=>), MonadTrans)
import qualified Data.ByteString as B
import Data.Default.Class
import qualified Data.Text as T
import Shelly (shelly, test_f, fromText)

import Scientific.Workflow.Serialization (Serializable(..))

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
    v <- case d of
        Nothing -> lift $ f x
        Just v -> return v
    suc l v
    return v

type IOProcessor = Processor (ReaderT Config IO)

type Actor = Kleisli IO

actor :: (a -> IO b) -> Actor a b
actor = Kleisli

-- | Source produce an output without taking inputs
type Source i = IOProcessor () i

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

-- | zip two sources
zipS :: Source a -> Source b -> Source (a,b)
zipS (Processor f) (Processor g) = Processor $ \_ -> do
    a <- f ()
    b <- g ()
    return (a,b)

zipS3 :: Source a -> Source b -> Source c -> Source (a,b,c)
zipS3 (Processor f) (Processor g) (Processor h) = Processor $ \_ -> do
    a <- f ()
    b <- g ()
    c <- h ()
    return (a,b,c)

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
