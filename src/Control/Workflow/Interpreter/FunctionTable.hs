{-# LANGUAGE TemplateHaskell #-}
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
import Data.Maybe
import Control.Monad.Reader
import qualified Control.Category as C
import Control.Distributed.Process (Process)

import Control.Workflow.Types

mkFunTable :: String   -- ^ Name of the table
           -> String   -- ^ SciFlow env i o
           -> Q [Dec]
mkFunTable nm wf = do
    funName <- newName nm
    let sig = fmap return $ funName `sigD`
            [t| (T.Text, B.ByteString, B.ByteString) -> Process (Maybe B.ByteString) |]
        fun = [d| $(varP funName) = liftIO . mkDict $(varE $ mkName wf) |]
        dec = [d| $(varP $ mkName nm) = FunctionTable $(mkClosure funName) $(functionTDict funName) $ __remoteTableDecl initRemoteTable |]
    remotableDecl [sig, fun, dec]

-- | Function table
type Dictionary = (T.Text, B.ByteString, B.ByteString)   -- ^ Input
                -> IO (Maybe B.ByteString)  -- ^ Output

mkDict :: Binary env
       => Free (Flow env) i o
       -> Dictionary
mkDict flow (nm, env, input) = unA $ eval go flow
  where
    go (Step job) = if nm == _job_name job
        then A $ do
            res <- runReaderT (_job_action job (decode input)) (decode env)
            return $ Just $ encode res
        else A $ return Nothing
    go _ = A $ return Nothing

-- | Helper type
newtype A m a b = A { unA :: m (Maybe B.ByteString) }

instance Monad m => C.Category (A m) where
    id = A (return Nothing)
    A a1 . A a2 = A $ do
        r1 <- a1
        r2 <- a2
        if isJust r1
            then return r1
            else return r2

instance Monad m => Arrow (A m) where
    arr _ = A (return Nothing)
    A a1 *** A a2 = A $ do
        r1 <- a1
        r2 <- a2
        if isJust r1
            then return r1
            else return r2
