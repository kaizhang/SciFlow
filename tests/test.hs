{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

import Control.Distributed.Process (Process, liftIO)
import qualified Data.ByteString.Lazy as B
import qualified Data.Text as T
import Data.Maybe
import Control.Distributed.Process.Closure (functionTDict, remotable, mkClosure, remotableDecl)
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Monad.Reader

import Control.Workflow.Coordinator.Local
import Control.Workflow.Types
import Control.Workflow.Language
import Control.Workflow.Language.TH

s0 = return . const 10

s1 :: Monad m => Int -> m Int
s1 = return . (*2)

s2 :: Monad m => Int -> m Int
s2 = return . (+2)

s3 :: Monad m => (Int, Int) -> m Int
s3 = return . uncurry (+)

s6 :: (Int,Int,Int) -> ReaderT Int IO Int
s6 (x,y,z) = do
    s <- ask
    return $ (x + y + z) * s

build "wf" [t| SciFlow Int |] $ do
    node "S0" 's0
    node "S1" 's1
    node "S2" 's2
    node "S3" 's3
    node "S4" 's0
    node "S5" 's0
    node "S6" 's6

    ["S3", "S4", "S5"] ~> "S6"
    ["S0"] ~> "S1"
    ["S0"] ~> "S2"
    ["S1", "S2"] ~> "S3"

main = mainWith defaultMainOpts{_n_workers = 5} 100 wf