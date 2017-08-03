{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE UndecidableInstances #-}
module Scientific.Workflow.Internal.DB
    ( openDB
    , closeDB
    , readData
    , readDataByteString
    , saveDataByteString
    , saveData
    , updateData
    , delRecord
    , isFinished
    , getKeys
    , WorkflowDB(..)
    , DBData (..)
    ) where

import qualified Data.ByteString        as B
import           Data.Maybe             (fromJust)
import qualified Data.Serialize         as S
import qualified Data.Text              as T
import           Data.Yaml              (FromJSON (..), ToJSON (..), decode,
                                         encode)
import           Database.SQLite.Simple
import           Text.Printf            (printf)

-- | An abstract type representing the database used to store states of workflow
newtype WorkflowDB  = WorkflowDB Connection

-- | 'DBData' type class is used for data serialization.
class DBData a where
    serialize :: a -> B.ByteString
    deserialize :: B.ByteString -> a
    showYaml :: a -> B.ByteString
    readYaml :: B.ByteString -> a

instance (FromJSON a, ToJSON a, S.Serialize a) => DBData a where
    serialize = S.encode
    deserialize = fromEither . S.decode
      where
        fromEither (Right x) = x
        fromEither _         = error "decode failed"
    showYaml = encode
    readYaml = fromJust . decode

type PID = T.Text

dbTableName :: String
dbTableName = "SciFlowDB"

createTable :: Connection -> String -> IO ()
createTable db tablename =
    execute_ db $ Query $ T.pack $ printf
        "CREATE TABLE %s(pid TEXT PRIMARY KEY, data BLOB)" tablename

hasTable :: Connection -> String -> IO Bool
hasTable db tablename = do
    r <- query db
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?" [tablename]
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

closeDB :: WorkflowDB -> IO ()
closeDB (WorkflowDB db) = close db
{-# INLINE closeDB #-}

readData :: DBData r => PID -> WorkflowDB -> IO r
readData pid (WorkflowDB db) = do
    [Only result] <- query db (Query $ T.pack $
        printf "SELECT data FROM %s WHERE pid=?" dbTableName) [pid]
    return $ deserialize result
{-# INLINE readData #-}

readDataByteString :: PID -> WorkflowDB -> IO B.ByteString
readDataByteString pid (WorkflowDB db) = do
    [Only result] <- query db (Query $ T.pack $
        printf "SELECT data FROM %s WHERE pid=?" dbTableName) [pid]
    return result
{-# INLINE readDataByteString #-}

saveData :: DBData r => PID -> r -> WorkflowDB -> IO ()
saveData pid result (WorkflowDB db) = execute db (Query $ T.pack $
    printf "INSERT INTO %s VALUES (?, ?)" dbTableName) (pid, serialize result)
{-# INLINE saveData #-}

updateData :: DBData r => PID -> r -> WorkflowDB -> IO ()
updateData pid result (WorkflowDB db) = execute db (Query $ T.pack $
    printf "UPDATE %s SET data=? WHERE pid=?" dbTableName) (serialize result, pid)
{-# INLINE updateData #-}

saveDataByteString :: PID -> B.ByteString -> WorkflowDB -> IO ()
saveDataByteString pid result (WorkflowDB db) = execute db (Query $ T.pack $
    printf "INSERT INTO %s VALUES (?, ?)" dbTableName) (pid, result)
{-# INLINE saveDataByteString #-}

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

delRecord :: PID -> WorkflowDB -> IO ()
delRecord pid (WorkflowDB db) =
    execute db (Query $ T.pack $ printf
        "DELETE FROM %s WHERE pid = ?" dbTableName) [pid]
