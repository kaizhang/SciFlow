{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GADTs #-}

module Control.Workflow.Main.Command.Remote (remote) where

import           Options.Applicative
import Control.Workflow.Coordinator
import Control.Workflow.Types
import Control.Workflow.Utils (mkNodeId)
import Data.Proxy (Proxy(..))

import Control.Workflow.Main.Types

data Remote a where
    Remote :: Coordinator coord =>
        { proxy :: Proxy coord
        , serverAddr :: String
        , serverPort :: Int
        } -> Remote coord

instance Command (Remote config) where
    runCommand Remote{..} = startClient proxy server . _function_table
      where
        server =  mkNodeId serverAddr serverPort

remote :: Coordinator coord
       => Proxy coord
       -> Parser Options
remote p = fmap Options $ Remote <$> pure p
    <*> strOption (long "ip")
    <*> option auto (long "port")
