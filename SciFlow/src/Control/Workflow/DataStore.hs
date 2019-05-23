{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveGeneric #-}
module Control.Workflow.DataStore
    ( DataStore(..)
    , Key(..)
    , mkKey
    , JobStatus(..)
    , openStore
    , closeStore
    , withStore
    , markPending
    , markFailed
    , saveItem
    , fetchItem
    , queryStatus
    ) where

import Control.Monad (unless)
import Control.Funflow.ContentHashable (ContentHash, encodeHash)
import Control.Concurrent.MVar
import Control.Monad.Catch (bracket)
import Data.Binary
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Catch (MonadMask)
import Database.SQLite.Simple
import GHC.Generics (Generic)
import qualified Data.HashMap.Strict as M
import qualified Data.Text as T
import qualified Data.ByteString.Char8 as B
import Text.Printf (printf)

newtype DataStore = DataStore { _db_conn :: MVar Connection }

data Key = Key { _hash :: B.ByteString, _name :: T.Text }

instance Show Key where
    show (Key hash jn) = T.unpack jn <> "(" <> h <> ")"
      where
        h = B.unpack (B.take 4 hash) <> ".."

mkKey :: ContentHash -> T.Text -> Key
mkKey hash nm = Key (encodeHash hash) nm
{-# INLINE mkKey #-}

data JobStatus = Pending
               | Complete
               | Failed String
               deriving (Eq, Generic, Show)

instance Binary JobStatus

openStore :: MonadIO m => FilePath -> m DataStore
openStore root = liftIO $ do
    db <- open root
    itemExist <- hasTable db "item_db"
    metaExist <- hasTable db "meta_db"
    unless (itemExist && metaExist) $ do
        execute_ db "CREATE TABLE item_db(hash TEXT PRIMARY KEY, data BLOB)"
        execute_ db
            "CREATE TABLE meta_db(hash TEXT PRIMARY KEY, jobname TEXT, status BLOB)"
        execute_ db "CREATE INDEX jobname_index ON meta_db(jobname)"
    DataStore <$> newMVar db
{-# INLINE openStore #-}

closeStore :: MonadIO m => DataStore -> m ()
closeStore (DataStore db) = liftIO $ withMVar db close
{-# INLINE closeStore #-}

withStore :: (MonadIO m, MonadMask m)
          => FilePath -> (DataStore -> m a) -> m a
withStore root = bracket (openStore root) closeStore
{-# INLINE withStore #-}

markPending :: MonadIO m => DataStore -> Key -> m ()
markPending (DataStore store) (Key k n) = liftIO $ withMVar store $ \db ->
    execute db "REPLACE INTO meta_db VALUES (?, ?, ?)" (k, n, encode Pending)
{-# INLINE markPending #-}

markFailed :: MonadIO m => DataStore -> Key -> String -> m ()
markFailed (DataStore store) (Key k n) msg  = liftIO $ withMVar store $ \db ->
    execute db "REPLACE INTO meta_db VALUES (?, ?, ?)" (k, n, encode $ Failed msg)
{-# INLINE markFailed #-}

queryStatus :: MonadIO m => DataStore -> Key -> m (Maybe JobStatus)
queryStatus (DataStore store) (Key k _) = liftIO $ withMVar store $ \db ->
    query db "SELECT status FROM meta_db WHERE hash=?" [k] >>= \case
        [Only result] -> return $ Just $ decode result
        _ -> return Nothing
{-# INLINE queryStatus #-}

saveItem :: (MonadIO m, Binary a) => DataStore -> Key -> a -> m ()
saveItem (DataStore store) (Key k n) res = liftIO $ withMVar store $ \db -> do
    execute db "REPLACE INTO item_db VALUES (?, ?)" (k, encode res)
    execute db "REPLACE INTO meta_db VALUES (?, ?, ?)" (k, n, encode Complete)
{-# INLINE saveItem #-}

fetchItem :: (MonadIO m, Binary a) => DataStore -> Key -> m a
fetchItem (DataStore store) (Key k n) = liftIO $ withMVar store $ \db -> do
    query db "SELECT data FROM item_db WHERE hash=?" [k] >>= \case
        [Only result] -> return $ decode result
{-# INLINE fetchItem #-}

-------------------------------------------------------------------------------
-- Low level functions
-------------------------------------------------------------------------------

type Val = B.ByteString

hasTable :: Connection -> String -> IO Bool
hasTable db tablename = do
    r <- query db
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?" [tablename]
    return $ not $ null (r :: [Only T.Text])
{-# INLINE hasTable #-}

{-
readData :: Key -> Connection -> IO Val
readData pid db = do
    [Only result] <- query db (Query $ T.pack $
        printf "SELECT data FROM %s WHERE pid=?" dbTableName) [pid]
    return result
{-# INLINE readData #-}

updateData :: Key -> Val -> Connection -> IO ()
updateData pid result db = execute db (Query $ T.pack $
    printf "UPDATE %s SET data=? WHERE pid=?" dbTableName) (result, pid)
{-# INLINE updateData #-}

saveData :: Key -> Val -> Connection -> IO ()
saveData pid result db = execute db (Query $ T.pack $
    printf "INSERT INTO %s VALUES (?, ?)" dbTableName) (pid, result)
{-# INLINE saveData #-}

isFinished :: Key -> Connection -> IO Bool
isFinished pid db = do
    result <- query db (Query $ T.pack $
        printf "SELECT pid FROM %s WHERE pid = ?" dbTableName) [pid]
    return $ not $ null (result :: [Only T.Text])
{-# INLINE isFinished #-}

getKeys :: Connection -> IO [Key]
getKeys db = concat <$> query_ db
    (Query $ T.pack $ printf "SELECT pid FROM %s;" dbTableName)
{-# INLINE getKeys #-}

delRecord :: Key -> Connection -> IO ()
delRecord pid db = execute db
    (Query $ T.pack $ printf "DELETE FROM %s WHERE pid = ?" dbTableName) [pid]
{-# INLINE delRecord #-}
-}