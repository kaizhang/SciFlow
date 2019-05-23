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
    , queryStatus
    ) where

import Control.Monad (unless)
import Control.Concurrent.MVar (withMVar, newMVar, MVar)
import Control.Monad.Catch (MonadMask, bracket)
import Control.Monad.Identity (Identity(..))
import Data.Binary (Binary, encode, decode)
import Data.Typeable (Typeable, typeOf)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Database.SQLite.Simple
import GHC.Generics (Generic)
import qualified Data.Text as T
import qualified Data.ByteString.Char8 as B
import qualified Crypto.Hash.SHA256 as C
import Data.ByteArray.Encoding (convertToBase, Base(..))

newtype DataStore = DataStore { _db_conn :: MVar Connection }

data Key = Key { _hash :: B.ByteString, _name :: T.Text }

instance Show Key where
    show (Key hash jn) = T.unpack jn <> "(" <> h <> ")"
      where
        h = B.unpack (B.take 4 hash) <> ".."

mkKey :: (Typeable i, Binary i) => i -> T.Text -> Key
mkKey input nm = Key hash nm
  where
    hash = convertToBase Base16 $ C.hashlazy $ encode (nm, show $ typeOf input, input)
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
fetchItem (DataStore store) (Key k _) = liftIO $ withMVar store $ \db -> do
    query db "SELECT data FROM item_db WHERE hash=?" [k] >>= \case
        [Only result] -> return $ decode result
        _ -> error "Item not found"
{-# INLINE fetchItem #-}

delRecord :: MonadIO m => DataStore -> Key -> m ()
delRecord (DataStore store) (Key k _) = liftIO $ withMVar store $ \db -> do
    execute db "DELETE FROM meta_db WHERE hash= ?" [k]
    execute db "DELETE FROM item_db WHERE hash= ?" [k]
{-# INLINE delRecord #-}

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