{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE UndecidableInstances #-}
module Scientific.Workflow.Internal.DB
    ( openDB
    , closeDB
    , readData
    , saveData
    , updateData
    , delRecord
    , isFinished
    , getKeys
    , WorkflowDB(..)
    , DBData
    , serialize
    , deserialize
    , readYaml
    , showYaml
    ) where

import qualified Data.ByteString        as B
import           Data.Maybe             (fromJust)
import qualified Data.Serialize         as S
import qualified Data.Text              as T
import           Data.Yaml              (FromJSON (..), ToJSON (..), decode,
                                         encode)
import           Database.SQLite.Simple
import           Text.Printf            (printf)

--------------------------------------------------------------------------------
-- Data Serialization
--------------------------------------------------------------------------------

-- | @DBData@ constraint is used for data serialization.
type DBData a = (FromJSON a, ToJSON a, S.Serialize a)

serialize :: DBData a => a -> B.ByteString
serialize = S.encode

deserialize :: DBData a => B.ByteString -> a
deserialize = fromEither . S.decode
  where
    fromEither (Right x) = x
    fromEither _         = error "decode failed"

showYaml :: DBData a => a -> B.ByteString
showYaml = encode

readYaml :: DBData a => B.ByteString -> a
readYaml = fromJust . decode


-- | An abstract type representing the database used to store states of workflow
newtype WorkflowDB  = WorkflowDB Connection
type Key = T.Text
type Val = B.ByteString


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

readData :: Key -> WorkflowDB -> IO Val
readData pid (WorkflowDB db) = do
    [Only result] <- query db (Query $ T.pack $
        printf "SELECT data FROM %s WHERE pid=?" dbTableName) [pid]
    return result
{-# INLINE readData #-}

updateData :: Key -> Val -> WorkflowDB -> IO ()
updateData pid result (WorkflowDB db) = execute db (Query $ T.pack $
    printf "UPDATE %s SET data=? WHERE pid=?" dbTableName) (result, pid)
{-# INLINE updateData #-}

saveData :: Key -> Val -> WorkflowDB -> IO ()
saveData pid result (WorkflowDB db) = execute db (Query $ T.pack $
    printf "INSERT INTO %s VALUES (?, ?)" dbTableName) (pid, result)
{-# INLINE saveData #-}

isFinished :: Key -> WorkflowDB -> IO Bool
isFinished pid (WorkflowDB db) = do
    result <- query db (Query $ T.pack $
        printf "SELECT pid FROM %s WHERE pid = ?" dbTableName) [pid]
    return $ not $ null (result :: [Only T.Text])
{-# INLINE isFinished #-}

getKeys :: WorkflowDB -> IO [Key]
getKeys (WorkflowDB db) = concat <$> query_ db (Query $ T.pack $
    printf "SELECT pid FROM %s;" dbTableName)
{-# INLINE getKeys #-}

delRecord :: Key -> WorkflowDB -> IO ()
delRecord pid (WorkflowDB db) =
    execute db (Query $ T.pack $ printf
        "DELETE FROM %s WHERE pid = ?" dbTableName) [pid]
