module Scientific.Workflow.Types where

import Control.Arrow (runKleisli, Kleisli(..))
import Control.Monad.Reader (ReaderT, lift, reader, (>=>))
import qualified Data.ByteString as B
import Data.Default.Class
import qualified Data.Text as T
import Shelly (shelly, test_f, fromText)

import Scientific.Workflow.Serialization (Serializable(..))

data Node a b = Node
    { runNode :: a -> ReaderT Config IO b
    }

type SourceNode i = Node () i

source :: o -> SourceNode o
source x = Node $ const $ return x

node :: Serializable b => String -> Kleisli IO a b -> Node a b
node label actor = Node $ \x -> do
    stored <- recover label
    case stored of
        Nothing -> do v <- lift $ runKleisli actor x
                      save label v
                      return v
        Just v -> return v

recover :: Serializable a => String -> ReaderT Config IO (Maybe a)
recover label = do
    dir <- reader _baseDir
    let file = dir ++ label
    exist <- lift $ fileExist file
    if exist
       then do c <- lift $ B.readFile file
               return $ deserialize c
       else return Nothing

save :: Serializable a => String -> a -> ReaderT Config IO ()
save label x = do
    dir <- reader _baseDir
    lift $ B.writeFile (dir++label) $ serialize x

fileExist :: FilePath -> IO Bool
fileExist x = shelly $ test_f $ fromText $ T.pack x

(~>) :: Node a b -> Node b c -> Node a c
(~>) (Node f) (Node g) = Node $ f >=> g

m2 :: Node () b -> Node () d -> Node () (b,d)
m2 (Node f) (Node g) = Node $ \_ -> do
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
        { _logDir = "workflow_log/"
        }
