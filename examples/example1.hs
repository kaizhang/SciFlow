{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

import Control.Monad.Reader
import Control.Concurrent (threadDelay)
import System.Environment

import Control.Workflow
import Control.Workflow.Coordinator.Local

s0 :: () -> ReaderT Int IO [Int]
s0 = return . const [1..10]

s1 :: Int -> ReaderT Int IO Int
s1 i = liftIO $ do
    threadDelay 5000000
    return i

s2 = return . (!!1)
s3 = return . (!!2)
s4 = return . (!!3)
s5 = return . (!!4)
s6 = return . (!!5)
s7 = return . (!!6)
s8 (a,b,c,d,e,f) = liftIO $ print [a,b,c,d,e,f]
s9 = liftIO . print 
    
build "wf" [t| SciFlow Int |] $ do
    node "S0" 's0
    nodePar "S1" 's1
    ["S0"] ~> "S1"

    node "S2" 's2
    node "S3" 's3
    node "S4" 's4
    node "S5" 's5
    node "S6" 's6
    node "S7" 's7
    ["S0"] ~> "S2"
    ["S0"] ~> "S3"
    ["S0"] ~> "S4"
    ["S0"] ~> "S5"
    ["S0"] ~> "S6"
    ["S0"] ~> "S7"

    node "S8" 's8
    ["S2", "S3", "S4", "S5","S6","S7"] ~> "S8"

    node "S9" 's9
    ["S1"] ~> "S9"

main :: IO ()
main = do
    [n] <- getArgs
    mainWith defaultMainOpts{_n_workers = read n} 100 wf