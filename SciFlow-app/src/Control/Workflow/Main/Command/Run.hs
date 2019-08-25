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
        , serverAddr :: Maybe String
        , serverPort :: Int
        , selection :: Maybe [T.Text]
        , nThread :: Maybe Int
        } -> Run (Config coord)

instance IsCommand (Run config) where
    runCommand Run{..} wf = do
        env <- decodeFileThrow configFile
        resource <- decodeFileThrow configFile
        case serverAddr of
            -- local mode
            Nothing -> withCoordinator (LocalConfig $ fromMaybe 1 nThread) $ \coord ->
                createTransport (defaultTCPAddr "localhost" (show serverPort))
                defaultTCPParameters >>= \case
                    Left ex -> errorS $ show ex
                    Right transport -> withStore dbPath $ \store ->
                        runSciFlow coord transport store
                            resource selection env wf
            -- Remote mode
            Just ip -> do
                config <- setQueueSize (fromMaybe 10 nThread) <$>
                    decodeConfig ip serverPort configFile
                withCoordinator config $ \coord ->
                    createTransport (defaultTCPAddr ip (show serverPort))
                        defaultTCPParameters >>= \case
                            Left ex -> errorS $ show ex
                            Right transport -> withStore dbPath $ \store ->
                                runSciFlow coord transport store resource selection env wf

run :: Coordinator coord
    => (String -> Int -> FilePath -> IO (Config coord))  -- ^ config reader
    -> Parser Command
run f1 = fmap Command $ Run <$> pure f1
    <*> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> help "Path to the workflow cache file."
       <> metavar "DB_PATH" )
    <*> strOption
        ( long "config"
       <> help "Workflow configuration file."
       <> metavar "CONFIG_PATH" )
    <*> (optional . strOption)
        ( long "ip"
       <> help "The ip address or hostname of the server."
       <> metavar "SERVER_ADDR" )
    <*> option auto
        ( long "port"
       <> short 'p'
       <> value 8888
       <> help "The port number."
       <> metavar "8888" )
    <*> (optional . option (T.splitOn "," . T.pack <$> str))
        ( long "select"
       <> metavar "NODE1,NODE2"
       <> help "Run only selected nodes.")
    <*> (optional . option auto)
        ( short 'n'
       <> help "The number of parallel threads/jobs."
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