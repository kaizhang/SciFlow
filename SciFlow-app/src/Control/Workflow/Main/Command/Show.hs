{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
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
import Control.Exception (try, SomeException(..))
import Control.Monad.Catch (handleAll)

import Control.Workflow.Main.Types
import Control.Workflow.DataStore
import Control.Workflow.Types

data Show' = Show'
    { stepName :: T.Text
    , dbPath :: FilePath }

instance IsCommand Show' where
    runCommand Show'{..} flow = withStore dbPath $ \store -> readEnv store >>= \case
        Nothing -> return ()
        Just env -> do
            cache <- newMVar M.empty
            let selection = getDependencies (_graph flow) stepName
                fun = showFlow cache (Just selection) store flow
            _ <- liftIO $ flip runReaderT env $ runExceptT $ runAsyncA fun ()
            takeMVar cache >>= printCache . M.filterWithKey f
      where
        f k _ = _name k == stepName
    
printCache :: M.HashMap Key T.Text -> IO ()
printCache cache = forM_ records $ \(k, x) -> do
    T.putStrLn $ "<< " <> k <> " >>"
    T.putStrLn $ x <> "\n"
  where
    records = sortBy (comparing fst) $ map (\(a,b) -> (T.pack $ showKey a, b)) $
        M.toList cache

show' :: Parser Command
show' = fmap Command $ Show'
    <$> strArgument
        ( metavar "STEP"
       <> help "The name of the step" ) 
    <*> strOption
        ( long "db-path"
       <> value "sciflow.db"
       <> metavar "DB_PATH" )

type FlowMonad env = ExceptT RunStatus (Env env)
data RunStatus = EarlyStopped | Errored

showFlow :: forall env. Binary env
         => MVar (M.HashMap Key T.Text)
         -> Maybe (S.HashSet T.Text)
         -> DataStore
         -> SciFlow env
         -> AsyncA (FlowMonad env) () ()
showFlow cache selection store sciflow = eval (AsyncA . runFlow') $ _flow sciflow
  where
    runFlow' (Step job) = if maybe True (S.member (_job_name job)) selection
        then runAsyncA $ eval (\(Action _) -> AsyncA $ \i ->
                let key = mkKey i $ _job_name job
                    f = queryStatus store key >>= \case
                        Complete dat -> do
                            let res = decode dat
                            modifyMVar_ cache $ \dict -> return $ if M.member key dict
                                then dict
                                else M.insert key (T.pack $ ppShow res) dict
                            return res
                        _ -> return $ error $ show $ _job_name job
                in liftIO $ try f >>= \case
                    Left (SomeException _) -> return $ error $ show $ _job_name job
                    Right x -> return x
            ) $ _job_action job
        else const $ return $ error $ show $ _job_name job
    runFlow' (UStep jn fun) = case selection of
        Nothing -> \i -> handleAll cleanUp $ lift $ fun i
        Just s -> if jn `S.member` s
            then \i -> handleAll cleanUp $ lift $ fun i
            else const $ return $ error $ show jn
      where
        cleanUp (SomeException ex) = throwError Errored
        {-
    runFlow' (UStep jn fun) = \i -> liftIO $ try (runReaderT (fun i) env) >>= \case
        Left (SomeException _) -> return $ error $ show jn
        Right x -> return x
        -}

getDependencies :: G.Gr NodeLabel () -> T.Text -> S.HashSet T.Text
getDependencies gr ids = S.map f $ go S.empty [hash ids]
  where
    f i = _label $ fromJust $ G.lab gr i
    go acc [] = acc 
    go acc xs = go (foldl' (flip S.insert) acc xs) parents
      where
        parents = concatMap (G.pre gr) xs
{-# INLINE getDependencies #-}