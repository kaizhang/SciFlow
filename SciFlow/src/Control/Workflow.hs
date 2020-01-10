--------------------------------------------------------------------------------
-- |
-- Module      :  Control.Workflow
-- Copyright   :  (c) 2015-2019 Kai Zhang
-- License     :  MIT
-- Maintainer  :  kai@kzhang.org
-- Stability   :  experimental
-- Portability :  portable
--
-- DSL for building computational workflows. Example:
--
-- > {-# LANGUAGE TemplateHaskell #-}
-- > {-# LANGUAGE OverloadedStrings #-}
-- >
-- > import Control.Monad.Reader
-- > import Control.Concurrent (threadDelay)
-- > import Control.Lens
-- > import System.Environment
-- > import qualified Data.HashMap.Strict as M
-- > import Network.Transport.TCP
-- > import Data.Proxy (Proxy(..))
-- >
-- > import Control.Workflow
-- > import Control.Workflow.Coordinator.Local
-- > import Control.Workflow.Coordinator.Drmaa
-- >
-- > s0 :: () -> ReaderT Int IO [Int]
-- > s0 = return . const [1..10]
-- >
-- > s1 :: Int -> ReaderT Int IO Int
-- >s1 i = (i*) <$> ask 
-- >
-- > s2 = return . (!!1)
-- > s3 = return . (!!2)
-- > s4 = return . (!!3)
-- > s5 = return . (!!4)
-- > s6 (a,b,c,d) = liftIO $ threadDelay 10000000 >>  print [a,b,c,d]
-- >     
-- > build "wf" [t| SciFlow Int |] $ do
-- >     node "S0" 's0 $ return ()
-- >     nodePar "S1" 's1 $ return ()
-- >     ["S0"] ~> "S1"
-- > 
-- >     node "S2" 's2 $ memory .= 30
-- >     node "S3" 's3 $ memory .= 30
-- >     node "S4" 's4 $ nCore .= 4
-- >     node "S5" 's5 $ queue .= Just "gpu"
-- >     ["S0"] ~> "S2"
-- >     ["S0"] ~> "S3"
-- >     ["S0"] ~> "S4"
-- >     ["S0"] ~> "S5"
-- > 
-- >     node "S6" 's6 $ return ()
-- >     ["S2", "S3", "S4", "S5"] ~> "S6"

-- > main :: IO ()
-- > main = do
-- >     let serverAddr = "192.168.0.1"
-- >         port = 8888
-- >         storePath = "sciflow.db"
-- >         resources = ResourceConfig $ M.fromList
-- >             [("S6", Resource (Just 2) Nothing Nothing)]
-- >     [mode] <- getArgs
-- >     case mode of
-- >         -- Run on local machine
-- >         "local" -> withCoordinator (LocalConfig 5) $ \coord -> do
-- >             Right transport <- createTransport (defaultTCPAddr serverAddr $ show port)
-- >                 defaultTCPParameters
-- >             withStore storePath $ \store -> 
-- >                 runSciFlow coord transport store (ResourceConfig M.empty) 2 wf
-- >         -- Using the DRMAA backend
-- >         "drmaa" -> do
-- >             config <- getDefaultDrmaaConfig ["slave"]
-- >             withCoordinator config $ \coord -> do
-- >                 Right transport <- createTransport (defaultTCPAddr serverAddr $ show port)
-- >                     defaultTCPParameters
-- >                 withStore storePath $ \store -> 
-- >                     runSciFlow coord transport store resources 2 wf
-- >         -- DRMAA workers
-- >         "slave" -> startClient (Proxy :: Proxy Drmaa)
-- >             (mkNodeId serverAddr port) $ _function_table wf
--------------------------------------------------------------------------------

module Control.Workflow
    ( -- * Workflow types
      SciFlow(..)
    , ResourceConfig(..)
    , Resource(..)

      -- * Construct workflow
    , Builder
    , node
    , nodePar
    , uNode
    , doc
    , nCore
    , memory
    , queue
    , (~>)
    , path
    , namespace
    , build
        
      -- * Run workflow
    , withCoordinator
    , withStore
    , startClient
    , runSciFlow
    , mkNodeId
    ) where

import Control.Workflow.Types
import Control.Workflow.Language
import Control.Workflow.Language.TH
import Control.Workflow.Interpreter.Exec
import Control.Workflow.DataStore
import Control.Workflow.Coordinator
import Control.Workflow.Utils