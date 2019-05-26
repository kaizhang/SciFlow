{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GADTs #-}
module Control.Workflow.Main.Command.Run (run) where

import Data.Yaml (decodeFileThrow)
import           Options.Applicative
import Control.Workflow.Coordinator
import Control.Workflow.Coordinator.Local (LocalConfig(..))
import Control.Workflow.Types
import Control.Workflow.DataStore
import Data.Maybe (fromJust)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Control.Workflow.Interpreter.Exec
import qualified Data.HashMap.Strict as M
import qualified Data.Text as T
import qualified Data.Text.IO as T

import Control.Workflow.Main.Types

data Run a where
    Run :: Coordinator coord =>
        { decodeConfig :: (String -> Int -> T.Text -> Config coord)
        , dbPath :: FilePath
        , configFile :: FilePath
        , isWorker :: Bool
        , serverAddr :: Maybe String
        , serverPort :: Int
        } -> Run (Config coord)

instance Command (Run config) where
    runCommand Run{..} wf = case serverAddr of
        -- local mode
        Nothing -> do
            env <- decodeFileThrow configFile
            withCoordinator (LocalConfig 1) $ \coord -> do
                    Right transport <- createTransport (defaultTCPAddr "localhost" (show serverPort))
                        defaultTCPParameters
                    withStore dbPath $ \store -> 
                        runSciFlow coord transport store (ResourceConfig M.empty) env wf
        Just ip -> do
            env <- decodeFileThrow configFile
            config <- decodeConfig ip serverPort <$> T.readFile configFile
            withCoordinator config $ \coord -> if isWorker
                then startClient coord $ _function_table wf
                else do
                    Right transport <- createTransport (defaultTCPAddr (fromJust serverAddr) (show serverPort))
                        defaultTCPParameters
                    withStore dbPath $ \store -> 
                        runSciFlow coord transport store (ResourceConfig M.empty) env wf

run :: Coordinator coord
    => (String -> Int -> T.Text -> Config coord)  -- ^ reader
    -> Parser Options
run f1 = fmap Options $ Run <$> pure f1
    <*> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> metavar "DB_PATH" )
    <*> strOption
        ( long "config"
       <> metavar "CONFIG_PATH" )
    <*> switch
        ( long "worker"
       <> help "Submit jobs to remote machines.")
    <*> (optional . strOption)
        ( long "ip"
       <> metavar "CONFIG_PATH" )
    <*> option auto
        ( long "port"
       <> short 'p'
       <> value 8888
       <> metavar "CONFIG_PATH" )