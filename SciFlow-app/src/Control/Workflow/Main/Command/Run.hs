{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE GADTs #-}
module Control.Workflow.Main.Command.Run (run) where

import Data.Yaml (decodeFileThrow)
import Data.Aeson
import           Data.Aeson.Types (parseEither)
import           Options.Applicative
import Data.Maybe (fromJust, fromMaybe)
import Network.Transport.TCP (createTransport, defaultTCPAddr, defaultTCPParameters)
import Control.Workflow.Interpreter.Exec
import qualified Data.HashMap.Strict as M
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Network.HostName (getHostName)
import Network.Socket.Free (getFreePort)

import Control.Workflow.Main.Types
import Control.Workflow.Coordinator
import Control.Workflow.Coordinator.Local (LocalConfig(..))
import Control.Workflow.Types
import Control.Workflow.Utils (errorS)
import Control.Workflow.DataStore

data Run a where
    Run :: Coordinator coord =>
        { decodeConfig :: (String -> Int -> FilePath -> IO (Config coord))
        , dbPath :: FilePath
        , configFile :: FilePath
        , useCloud :: Bool
        , serverAddr :: Maybe String
        , serverPort :: Maybe Int
        , selection :: Maybe [T.Text]
        , nThread :: Maybe Int
        } -> Run (Config coord)

instance IsCommand (Run config) where
    runCommand Run{..} wf = do
        env <- decodeFileThrow configFile
        port <- case serverPort of
            Nothing -> getFreePort
            Just p -> return p
        if useCloud then runCloud env port else runLocal env port
      where
        runLocal env port = withCoordinator (LocalConfig $ fromMaybe 1 nThread) $ \coord ->
            createTransport (defaultTCPAddr "localhost" (show port))
            defaultTCPParameters >>= \case
                Left ex -> errorS $ show ex
                Right transport -> withStore dbPath $ \store ->
                    runSciFlow coord transport store
                        (ResourceConfig M.empty) selection env wf
        runCloud env port = do
            resource <- decodeFileThrow configFile
            ip <- case serverAddr of
                Nothing -> getHostName
                Just x -> return x
            config <- setQueueSize (fromMaybe 10 nThread) <$>
                decodeConfig ip port configFile
            createTransport (defaultTCPAddr ip (show port)) defaultTCPParameters >>= \case
                Left ex -> errorS $ show ex
                Right transport -> withCoordinator config $ \coord ->
                    withStore dbPath $ \store -> runSciFlow coord transport
                        store resource selection env wf

run :: Coordinator coord
    => (String -> Int -> FilePath -> IO (Config coord))  -- ^ config reader
    -> Parser Command
run f1 = fmap Command $ Run <$> pure f1
    <*> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> help "Path to the workflow cache file"
       <> metavar "DB_PATH" )
    <*> strOption
        ( long "config"
       <> help "Workflow configuration file"
       <> metavar "CONFIG_PATH" )
    <*> switch
        ( long "cloud"
       <> help "Use distributed computing" )
    <*> (optional . strOption)
        ( long "master-ip"
       <> help "The ip address of the master server. The default uses the hostname"
       <> metavar "MASTER_ADDR" )
    <*> (optional . option auto)
        ( long "port"
       <> short 'p'
       <> help "The port number"
       <> metavar "PORT" )
    <*> (optional . option (T.splitOn "," . T.pack <$> str))
        ( long "select"
       <> metavar "NODE1,NODE2"
       <> help "Run only selected nodes")
    <*> (optional . option auto)
        ( short 'n'
       <> help "The number of parallel threads/jobs"
       <> metavar "JOBS" )

instance FromJSON Resource where
    parseJSON = withObject "Resource" $ \v -> Resource
        <$> v .:? "cpu"
        <*> v .:? "memory"
        <*> v .:? "parameter"

instance FromJSON ResourceConfig where
    parseJSON = withObject "ResourceConfig" $ \obj ->
        return $ case M.lookup "resource" obj of
            Nothing -> ResourceConfig M.empty
            Just res -> ResourceConfig $ case parseEither parseJSON res of
                Left err -> error err
                Right r  -> r