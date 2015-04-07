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
newtype Proc m a b = Proc { runProc :: a -> m b }

instance Monad m => C.Category (Proc m) where
    id = Proc return
    (Proc f) . (Proc g) = Proc $ g >=> f

instance Monad m => Arrow (Proc m) where
    arr f = Proc (return . f)
    first (Proc f) = Proc (\ ~(b,d) -> f b >>= \c -> return (c,d))
    second (Proc f) = Proc (\ ~(d,b) -> f b >>= \c -> return (d,c))

-- | Label is a pair of side effects
type Label m l o = (l -> m (Maybe o), l -> o -> m ())

-- | Turn a Kleisli arrow into a labeled arrow
label :: (MonadTrans t, Monad m, Monad (t m))
      => Label (t m) l b
      -> l
      -> Kleisli m a b
      -> Proc (t m) a b
label (pre, suc) l (Kleisli f) = Proc $ \x -> do
    d <- pre l
    v <- case d of
        Nothing -> lift $ f x
        Just v -> return v
    suc l v
    return v

type IOProc = Proc (ReaderT Config IO)

type Actor = Kleisli IO

-- | Source Proc produce an output without taking inputs
type SourceProc i = IOProc () i

proc :: Serializable b => String -> Kleisli IO a b -> IOProc a b
proc = label (recover, save)

source :: Serializable o => String -> o -> SourceProc o
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

m2 :: SourceProc a -> SourceProc b -> SourceProc (a,b)
m2 (Proc f) (Proc g) = Proc $ \_ -> do
    a <- f ()
    b <- g ()
    return (a,b)

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
