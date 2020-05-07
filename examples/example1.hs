{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

import Control.Monad.Reader
import Control.Concurrent (threadDelay)
import System.Environment
import qualified Data.HashMap.Strict as M
import Network.Transport.TCP
import Data.Proxy (Proxy(..))

import Control.Workflow
import Control.Workflow.Coordinator.Local
import Control.Workflow.Coordinator.Remote
import Control.Workflow.Main.Command.View
import Control.Workflow.Interpreter.Graph

s0 :: () -> ReaderT Int IO [Int]
s0 = return . const [1..10]

s1 :: Int -> ReaderT Int IO Int
s1 i = (i*) <$> ask 

s2 = return . (!!1)
s3 = return . (!!2)
s4 = return . (!!3)
s6 (a,b,c,d) = liftIO $ threadDelay 5000000 >>  print [a,b,c,d]
    
build "wf" [t| SciFlow Int |] $ do
    node "S0" 's0 $ return ()
    nodePar "S1" 's1 $ return ()
    ["S0"] ~> "S1"

    node "S2" 's2 $ return ()
    node "S3" 's3 $ return ()
    node "S4" 's4 $ return ()
    uNode "S5" [| return . (!!4) |]
    ["S0"] ~> "S2"
    ["S0"] ~> "S3"
    ["S0"] ~> "S4"
    ["S0"] ~> "S5"

    node "S6" 's6 $ return ()
    ["S2", "S3", "S4", "S5"] ~> "S6"

main :: IO ()
main = do
    writeFile "workflow.html" $ renderGraph $ mkGraph wf
    let serverAddr = "192.168.0.1"
        port = 23488
        storePath = "sciflow.db"
        resources = ResourceConfig $ M.fromList
            [("S6", Resource (Just 2) Nothing Nothing)]
    [mode] <- getArgs
    case mode of
        -- Run on local machine
        "local" -> withCoordinator (LocalConfig 5) $ \coord ->
                createTransport (defaultTCPAddr "localhost" $ show port) defaultTCPParameters >>= \case
                    Left ex -> print ex
                    Right transport -> withStore storePath $ \store -> 
                        runSciFlow coord transport store (ResourceConfig M.empty) (Just ["S6"]) 2 wf
        -- Using the Remote backend
        "remote" -> do
            config <- getDefaultRemoteConfig ["slave"]
            withCoordinator config $ \coord -> do
                Right transport <- createTransport (defaultTCPAddr serverAddr $ show port)
                    defaultTCPParameters
                withStore storePath $ \store -> 
                    runSciFlow coord transport store resources Nothing 2 wf

        -- DRMAA workers
        "slave" -> startClient (Proxy :: Proxy Remote)
            (mkNodeId serverAddr port) $ _function_table wf
