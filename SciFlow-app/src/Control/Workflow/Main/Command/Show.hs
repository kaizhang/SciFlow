{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE BangPatterns #-}

module Control.Workflow.Main.Command.Show (show') where

import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Control.Arrow.Async
import Control.Arrow.Free (eval)
import Data.Binary (Binary, decode)
import           Options.Applicative
import qualified Data.HashMap.Strict as M
import Text.Show.Pretty (ppShow)
import Control.Monad.Except
import Control.Monad.Reader (runReaderT)
import Control.Concurrent.MVar
import Data.List
import Data.Ord
import qualified Data.Graph.Inductive as G
import qualified Data.HashSet as S
import Data.Maybe
import Data.Hashable (hash)

import Control.Workflow.Main.Types
import Control.Workflow.DataStore
import Control.Workflow.Types

data Show' = Show'
    { stepName :: Maybe T.Text
    , dbPath :: FilePath }

instance IsCommand Show' where
    runCommand Show'{..} flow = withStore dbPath $ \store -> readEnv store >>= \case
        Nothing -> return ()
        Just env -> do
            cache <- newMVar M.empty
            let selection = fmap (getDependencies (_graph flow)) stepName
            _ <- runExceptT $ runAsyncA (showFlow cache selection store env flow) ()
            takeMVar cache >>= printCache . M.filterWithKey f
      where
        f k _ = case stepName of
            Nothing -> True
            Just nm -> _name k == nm
    
printCache :: M.HashMap Key T.Text -> IO ()
printCache cache = forM_ records $ \(k, x) -> do
    T.putStrLn $ "<< " <> k <> " >>"
    T.putStrLn $ x <> "\n"
  where
    records = sortBy (comparing fst) $ map (\(a,b) -> (T.pack $ showKey a, b)) $
        M.toList cache

show' :: Parser Command
show' = fmap Command $ Show'
    <$> (optional . strArgument)
        ( metavar "STEP"
       <> help "The name of the step" ) 
    <*> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> metavar "DB_PATH" )

showFlow :: Binary env
         => MVar (M.HashMap Key T.Text)
         -> Maybe (S.HashSet T.Text)
         -> DataStore
         -> env
         -> SciFlow env
         -> AsyncA (ExceptT () IO) () ()
showFlow cache selection store env sciflow = eval (AsyncA . runFlow') $ _flow sciflow
  where
    runFlow' (Step job) = runAsyncA $ eval ( \(Action _) ->
        AsyncA $ \ !i -> if maybe True (S.member (_job_name job)) selection
            then do
                let key = mkKey i $ _job_name job
                lift (queryStatus store key) >>= \case
                    Complete dat -> do
                        let res = decode dat
                        lift $ modifyMVar_ cache $ \dict -> return $ if M.member key dict
                            then dict
                            else M.insert key (T.pack $ ppShow res) dict
                        return res
                    _ -> return undefined
            else return undefined
        ) $ _job_action job
    runFlow' (UStep fun) = \i -> lift $ runReaderT (fun i) env

getDependencies :: G.Gr (Maybe NodeLabel) () -> T.Text -> S.HashSet T.Text
getDependencies gr ids = S.map fromJust $ S.filter isJust $ S.map f $ go S.empty [hash ids]
  where
    f i = fmap _label $ fromJust $ G.lab gr i
    go acc [] = acc 
    go acc xs = go (foldl' (flip S.insert) acc xs) parents
      where
        parents = concatMap (G.pre gr) xs
{-# INLINE getDependencies #-}

