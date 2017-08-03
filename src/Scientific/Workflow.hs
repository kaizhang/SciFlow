module Scientific.Workflow
    ( runWorkflow
    , module Scientific.Workflow.Internal.Builder
    , module Scientific.Workflow.Internal.Builder.Types
    , module Scientific.Workflow.Types
    ) where

import           Control.Concurrent                         (forkIO)
import           Control.Concurrent.MVar
import           Control.Exception                          (bracket,
                                                             displayException)
import           Control.Monad.Reader                       (runReaderT)
import           Control.Monad.State
import           Control.Monad.Trans.Except
import qualified Data.ByteString.Char8                      as B
import           Data.Default.Class                         (Default (..))
import           Data.Graph.Inductive.Graph                 (lab, labNodes)
import           Data.Graph.Inductive.Query.DFS             (rdfs)
import qualified Data.Map                                   as M
import           Data.Maybe                                 (fromJust)
import qualified Data.Set                                   as S
import           Data.Tuple                                 (swap)
import           Data.Yaml                                  (FromJSON, decode)
import           Text.Printf                                (printf)

import           Scientific.Workflow.Internal.Builder
import           Scientific.Workflow.Internal.Builder.Types
import           Scientific.Workflow.Internal.DB
import           Scientific.Workflow.Internal.Utils
import           Scientific.Workflow.Types

runWorkflow :: (Default config, FromJSON config)
            => Workflow config -> RunOpt -> IO ()
runWorkflow (Workflow gr pids wf) opts =
    bracket (openDB $ dbFile opts) closeDB $ \db -> do
        ks <- S.fromList <$> getKeys db
        let selection = case selected opts of
                Nothing -> Nothing
                Just xs -> let nodeMap = M.fromList $ map swap $ labNodes gr
                               nds = map (flip (M.findWithDefault undefined) nodeMap) xs
                           in Just $ S.fromList $ map (fromJust . lab gr) $ rdfs nds gr

        pidStateMap <- flip M.traverseWithKey pids $ \pid attr ->
            case runMode opts of
                Master -> do
                    v <- case fmap (S.member pid) selection of
                        Just False -> newMVar $ Special Skip
                        _ -> if pid `S.member` ks
                            then newMVar Success
                            else newMVar Scheduled
                    return (v, attr)
                Slave i input output -> do
                    v <- if pid == i
                        then newMVar $ Special $ EXE input output
                        else newMVar $ Special Skip
                    return (v, attr)
                Review i -> do
                    v <- if pid == i then newMVar (Special FetchData) else newMVar $ Special Skip
                    return (v, attr)
                Replace i input -> do
                    v <- if pid == i then newMVar (Special $ WriteData input) else newMVar $ Special Skip
                    return (v, attr)

        para <- newEmptyMVar
        _ <- forkIO $ replicateM_ (nThread opts) $ putMVar para ()

        let initState = WorkflowState db pidStateMap para (runOnRemote opts)
        config <- case configuration opts of
            [] -> return def
            fls -> do
                r <- decode . B.unlines <$> mapM B.readFile fls
                case r of
                    Nothing -> error "fail to parse configuration file"
                    Just x  -> return x
        result <- evalStateT (runExceptT $ runReaderT (wf ()) initState) config
        case result of
            Right _ -> return ()
            Left (pid, ex) -> errorMsg $ printf "\"%s\" failed. The error was: %s."
                pid (displayException ex)
