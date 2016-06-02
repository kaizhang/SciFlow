{-# LANGUAGE OverloadedStrings #-}
module Scientific.Workflow.DB
    ( openDB
    , readData
    , saveData
    , isFinished
    , getKeys
    ) where

import Scientific.Workflow.Types
import qualified Data.ByteString     as B
import qualified Data.Text           as T
import Database.SQLite.Simple
import Text.Printf (printf)

dbTableName :: String
dbTableName = "SciFlowDB"

createTable :: Connection -> String -> IO ()
createTable db tablename =
    execute_ db $ Query $ T.pack $ printf
        "CREATE TABLE %s(pid TEXT PRIMARY KEY, data BLOB)" tablename

hasTable :: Connection -> String -> IO Bool
hasTable db tablename = do
    r <- query db
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?" [tablename]
    return $ not $ null (r :: [Only T.Text])

openDB :: FilePath -> IO WorkflowDB
openDB dbFile = do
    db <- open dbFile
    exist <- hasTable db dbTableName
    if exist
        then return $ WorkflowDB db
        else do
            createTable db dbTableName
            return $ WorkflowDB db
{-# INLINE openDB #-}

readData :: Serializable r => PID -> WorkflowDB -> IO r
readData pid (WorkflowDB db) = do
    [Only result] <- query db (Query $ T.pack $
        printf "SELECT data FROM %s WHERE pid = ?" dbTableName) [pid]
    return $ deserialize result
{-# INLINE readData #-}

saveData :: Serializable r => PID -> r -> WorkflowDB -> IO ()
saveData pid result (WorkflowDB db) = execute db (Query $ T.pack $
    printf "INSERT INTO %s VALUES (?, ?)" dbTableName) (pid, serialize result)
{-# INLINE saveData #-}

isFinished :: PID -> WorkflowDB -> IO Bool
isFinished pid (WorkflowDB db) = do
    result <- query db (Query $ T.pack $
        printf "SELECT pid FROM %s WHERE pid = ?" dbTableName) [pid]
    return $ not $ null (result :: [Only T.Text])
{-# INLINE isFinished #-}

getKeys :: WorkflowDB -> IO [PID]
getKeys (WorkflowDB db) = concat <$> query_ db (Query $ T.pack $
    printf "SELECT pid FROM %s;" dbTableName)
{-# INLINE getKeys #-}
