{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
module Control.Workflow.DataStore
    ( DataStore(..)
    , Key
    , mkKey
    , JobStatus(..)
    , openStore
    , closeStore
    , withStore
    , markPending
    , markFailed
    , saveItem
    , fetchItem
    , delRecord
    , delJobs
    , queryStatus
    , queryStatusPending
    ) where

import Control.Monad (unless)
import Control.Concurrent.MVar (withMVar, newMVar, MVar)
import Control.Monad.Catch (MonadMask, bracket)
import Data.Binary (Binary, encode, decode)
import Data.Typeable (Typeable, typeOf)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Database.SQLite.Simple
import GHC.Generics (Generic)
import qualified Data.Text as T
import qualified Data.ByteString.Char8 as B
import qualified Crypto.Hash.SHA256 as C
import Data.ByteArray.Encoding (convertToBase, Base(..))

-- | Data store.
newtype DataStore = DataStore { _db_conn :: MVar Connection }

-- | A key is uniquely determined by the input hash and the name of the job.
data Key = Key { _hash :: B.ByteString, _name :: T.Text }

instance Show Key where
    show (Key hash jn) = T.unpack jn <> "(" <> h <> ")"
      where
        h = B.unpack (B.take 4 hash) <> ".."

-- | Create the key from input and job name.
mkKey :: (Typeable i, Binary i) => i -> T.Text -> Key
mkKey input nm = Key hash nm
  where
    hash = convertToBase Base16 $ C.hashlazy $ encode (nm, show $ typeOf input, input)
{-# INLINE mkKey #-}

-- | The status of jobs.
data JobStatus = Pending
               | Complete
               | Failed String
               deriving (Eq, Generic, Show)

instance Binary JobStatus

-- | Open the store.
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

-- | Close the store and release resource.
closeStore :: MonadIO m => DataStore -> m ()
closeStore (DataStore db) = liftIO $ withMVar db close
{-# INLINE closeStore #-}

withStore :: (MonadIO m, MonadMask m)
          => FilePath -> (DataStore -> m a) -> m a
withStore root = bracket (openStore root) closeStore
{-# INLINE withStore #-}

-- | Mark a given job as pending.
markPending :: MonadIO m => DataStore -> Key -> m ()
markPending (DataStore store) (Key k n) = liftIO $ withMVar store $ \db ->
    execute db "REPLACE INTO meta_db VALUES (?, ?, ?)" (k, n, encode Pending)
{-# INLINE markPending #-}

-- | Mark a given job as failed.
markFailed :: MonadIO m => DataStore -> Key -> String -> m ()
markFailed (DataStore store) (Key k n) msg  = liftIO $ withMVar store $ \db ->
    execute db "REPLACE INTO meta_db VALUES (?, ?, ?)" (k, n, encode $ Failed msg)
{-# INLINE markFailed #-}

-- | Get the status of a given job.
queryStatus :: MonadIO m => DataStore -> Key -> m (Maybe JobStatus)
queryStatus (DataStore store) (Key k _) = liftIO $ withMVar store $ \db ->
    query db "SELECT status FROM meta_db WHERE hash=?" [k] >>= \case
        [Only result] -> return $ Just $ decode result
        _ -> return Nothing
{-# INLINE queryStatus #-}

-- | Get the status of a given job and mark the job as pending if missing.
queryStatusPending :: MonadIO m => DataStore -> Key -> m (Maybe JobStatus)
queryStatusPending (DataStore store) (Key k n) = liftIO $ withMVar store $ \db ->
    query db "SELECT status FROM meta_db WHERE hash=?" [k] >>= \case
        [Only result] -> return $ Just $ decode result
        _ -> do
            execute db "REPLACE INTO meta_db VALUES (?, ?, ?)" (k, n, encode Pending)
            return Nothing
{-# INLINE queryStatusPending #-}

-- | Save the data of a given job.
saveItem :: (MonadIO m, Binary a) => DataStore -> Key -> a -> m ()
saveItem (DataStore store) (Key k n) res = liftIO $ withMVar store $ \db -> do
    execute db "REPLACE INTO item_db VALUES (?, ?)" (k, encode res)
    execute db "REPLACE INTO meta_db VALUES (?, ?, ?)" (k, n, encode Complete)
{-# INLINE saveItem #-}

-- | Get the data of a given job.
fetchItem :: (MonadIO m, Binary a) => DataStore -> Key -> m a
fetchItem (DataStore store) (Key k _) = liftIO $ withMVar store $ \db -> do
    query db "SELECT data FROM item_db WHERE hash=?" [k] >>= \case
        [Only result] -> return $ decode result
        _ -> error "Item not found"
{-# INLINE fetchItem #-}

-- | Delete a record based on the key.
delRecord :: MonadIO m => DataStore -> Key -> m ()
delRecord (DataStore store) (Key k _) = liftIO $ withMVar store $ \db -> do
    execute db "DELETE FROM meta_db WHERE hash= ?" [k]
    execute db "DELETE FROM item_db WHERE hash= ?" [k]
{-# INLINE delRecord #-}

-- | Delete all records with the given job name.
delJobs :: MonadIO m
        => DataStore
        -> T.Text     -- ^ job name
        -> m ()
delJobs (DataStore store) jn = liftIO $ withMVar store $ \db -> undefined
{-# INLINE delJobs #-}

-------------------------------------------------------------------------------
-- Low level functions
-------------------------------------------------------------------------------

hasTable :: Connection -> String -> IO Bool
hasTable db tablename = do
    r <- query db
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?" [tablename]
    return $ not $ null (r :: [Only T.Text])
{-# INLINE hasTable #-}

{-
getKeys :: Connection -> IO [Key]
getKeys db = concat <$> query_ db
    (Query $ T.pack $ printf "SELECT pid FROM %s;" dbTableName)
-}