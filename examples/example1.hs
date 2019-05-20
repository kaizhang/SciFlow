{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

import Control.Monad.Reader
import Control.Concurrent (threadDelay)
import Control.Lens
import qualified Data.Text.Lazy.IO                             as TL
import System.Environment
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.HashMap.Strict as M
import Control.Workflow.Interpreter.Graph (mkGraph)
import Control.Workflow.Visualize 

import Control.Workflow
import Control.Workflow.Coordinator.Local
--import Control.Workflow.Coordinator.Drmaa

sx :: () -> ReaderT Int IO String
sx = return . const "TEST"

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
s8 (a,b,c,d,e,f) = liftIO $ threadDelay 10000000 >>  print [a,b,c,d,e,f]
s9 = liftIO . print 
    
build "wf" [t| SciFlow Int |] $ do
    node "SX" 'sx $ return ()

    node "S0" 's0 $ return ()
    nodePar "S1" [| \x -> {- error (show x) >> -} s1 x |] $ return ()
    ["S0"] ~> "S1"

    node "S2" 's2 $ memory .= 30
    node "S3" 's3 $ memory .= 30
    node "S4" 's4 $ nCore .= 4
    node "S5" 's5 $ queue .= Just "gpu"
    node "S6" 's6 $ return ()
    node "S7" 's7 $ return ()
    ["S0"] ~> "S2"
    ["S0"] ~> "S3"
    ["S0"] ~> "S4"
    ["S0"] ~> "S5"
    ["S0"] ~> "S6"
    ["S0"] ~> "S7"

    node "S8" 's8 $ return ()
    ["S2", "S3", "S4", "S5","S6","S7"] ~> "S8"

    node "S9" 's9 $ return ()
    ["S1"] ~> "S9"

main :: IO ()
main = do
    [n] <- getArgs
    let opt = defaultOpts
            { _n_workers = read n
            , _resources = ResourceConfig $
                M.fromList [("S6", Resource (Just 2) Nothing Nothing)]
            }
    writeFile "example1.html" $ renderCytoscape $ mkGraph wf
    mainWith opt 100 wf