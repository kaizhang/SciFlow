{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE LambdaCase #-}
-- This interpreter treat a workflow as a function lookup table. 
-- Given a (key, state, input) tuple where state and input are in binary format
-- , the table will return the output in binary format.

module Control.Workflow.Interpreter.FunctionTable
    ( mkDict
    , mkFunTable
    ) where

import Data.Binary
import           Language.Haskell.TH
import qualified Data.ByteString.Lazy as B
import qualified Data.Text as T
import Control.Arrow.Free (Free, eval)
import Control.Arrow
import Control.Distributed.Process.Closure (functionTDict, mkClosure, remotableDecl)
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Monad.Reader
import qualified Control.Category as C
import Control.Distributed.Process (Process)
import Control.Concurrent.MVar

import Control.Workflow.Types

mkFunTable :: String   -- ^ Name of the table
           -> String   -- ^ SciFlow env i o
           -> Q [Dec]
mkFunTable nm wf = remotableDecl [sig, fun, dec]
  where
    funName = mkName $ nm ++ "__dict"
    sig = fmap return $ funName `sigD`
        [t| (T.Text, B.ByteString, B.ByteString) -> Process (Maybe B.ByteString) |]
    fun = [d| $(varP funName) = liftIO . mkDict $(varE $ mkName wf) |]
    dec = [d| $(varP $ mkName nm) = FunctionTable $(mkClosure funName) $(functionTDict funName) $ __remoteTableDecl initRemoteTable |]

-- | Function table
type Dictionary = (T.Text, B.ByteString, B.ByteString)   -- ^ Input
                -> IO (Maybe B.ByteString)  -- ^ Output

mkDict :: Binary env
       => Free (Flow env) i o
       -> Dictionary
mkDict flow (nm, env, input) = do
    res <- newMVar Nothing
    unA $ eval (go res) flow
    readMVar res
  where
    go res (Step job) = A $ modifyMVar_ res $ \case
        Nothing -> if nm == _job_name job
            then runJob job
            else return Nothing
        x -> return x
      where
    go _ _ = A $ return ()
    runJob job = Just . encode <$>
        runReaderT (f (decode input)) (decode env)
      where
        f = runKleisli $ eval (Kleisli . _unAction) $ _job_action job

-- | Helper type
data A a b = A { unA :: IO () }

instance C.Category A where
    id = A $ return ()
    A f . A g = A (g >> f)

instance Arrow A where
    arr _ = A $ return ()
    A f *** A g = A (f >> g)

instance ArrowChoice A where
    left (A a) = A a