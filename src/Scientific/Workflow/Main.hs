{-# LANGUAGE CPP               #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Scientific.Workflow.Main
    ( defaultMain
    , defaultMainOpts
    , mainWith
    , MainOpts(..)
    , runWorkflow
    ) where

import           Control.Concurrent                         (forkIO)
import           Control.Concurrent.MVar
import           Control.Exception                          (bracket,
                                                             displayException)
import           Control.Monad                              (replicateM_)
import           Control.Monad.Reader                       (runReaderT)
import           Control.Monad.Trans.Except                 (runExceptT)
import           Data.Default.Class                         (Default (..))
import           Data.Graph.Inductive.Graph                 (lab, labNodes,
                                                             nmap)
import           Data.Graph.Inductive.Query.DFS             (rdfs)
import qualified Data.Map                                   as M
import           Data.Maybe                                 (fromJust)
import qualified Data.Set                                   as S
import           Data.Tuple                                 (swap)
import           Network.Socket                             (Family (..),
                                                             SockAddr (..),
                                                             Socket,
                                                             SocketType (Stream),
                                                             close, connect,
                                                             defaultProtocol,
                                                             isConnected,
                                                             socket)

import qualified Data.ByteString.Char8                      as B
import           Data.Graph.Inductive.PatriciaTree          (Gr)
import           Data.Maybe                                 (fromMaybe)
import           Data.Serialize                             (encode)
import qualified Data.Text                                  as T
import qualified Data.Text.Lazy.IO                          as T
import           Data.Yaml                                  (FromJSON, decodeEither)

#ifdef DRMAA_ENABLED
import           DRMAA                                      (withSession)
#endif

import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift                   as T
import           Options.Applicative                        hiding (Success)
import           Text.Printf                                (printf)

import           Data.Version                               (showVersion)
import           Paths_SciFlow                              (version)
import           Scientific.Workflow.Internal.Builder
import           Scientific.Workflow.Internal.Builder.Types
import           Scientific.Workflow.Internal.DB
import           Scientific.Workflow.Internal.Utils
import           Scientific.Workflow.Main.Options           (CMD (..),
                                                             GlobalOpts (..),
                                                             argsParser)
import           Scientific.Workflow.Types
import           Scientific.Workflow.Visualize

data MainOpts = MainOpts
    { preAction     :: Name   -- ^ An action to be execute before the workflow.
                              -- The action should have type: @'IO' () -> 'IO' ()@.
                              -- e.g., some initialization processes.
    , programHeader :: String  -- ^ Short description about the program.
    , workflowConfigType :: Maybe Name    -- ^ The type of workflow config. Default is
                                          -- @Nothing@ which let type inference do its job.
    }

T.deriveLift ''MainOpts

defaultMainOpts :: MainOpts
defaultMainOpts = MainOpts
    { preAction = 'id
    , programHeader = printf "SciFlow-%s" (showVersion version)
    , workflowConfigType = Nothing
    }

defaultMain :: Builder () -> Q [Dec]
defaultMain = mainWith defaultMainOpts

mainWith :: MainOpts -> Builder () -> Q [Dec]
mainWith opts builder = do
    wf_q <- buildWorkflow wfName builder
    main_q <- [d| main = mainFunc $(varE $ preAction opts) dag
                    $(varE $ mkName wfName) (programHeader opts)
              |]
    return $ wfType ++ wf_q ++ main_q
  where
    wfType = case workflowConfigType opts of
        Nothing -> []
        Just ty -> [SigD (mkName wfName) $ AppT (ConT $ mkName "Workflow") $
            ConT ty]
    wfName = "sciFlowDefaultMain"
    dag = nmap (\x -> (_nodePid x, _nodeAttr x)) $ mkDAG builder
{-# INLINE mainWith #-}

mainFunc :: (Default config, FromJSON config)
         => (IO () -> IO ()) -- initialization function
         -> Gr (PID, Attribute) Int -> Workflow config
         -> String  -- program header
         -> IO ()
mainFunc initialize dag wf h = execParser (argsParser h) >>= execute
  where
    execute cmd = case cmd of
        Run opts n r s logS ->
            let runOpts = defaultRunOpt
                    { dbFile = dbPath opts
                    , runOnRemote = True
                    , nThread = n
                    , configuration = fromMaybe [] $ configFile opts
                    , selected = fmap (map T.pack) s
                    , logServerAddr = logS }
            in if r
#ifdef DRMAA_ENABLED
                then initialize $ withSession $ runWorkflow wf runOpts
#else
                then initialize $ runWorkflow wf runOpts
#endif
                else runWorkflow wf runOpts{runOnRemote = False}

        View isRaw -> if isRaw
            then B.putStr $ encode dag
            else T.putStrLn $ drawWorkflow dag

        Cat opts pid -> runWorkflow wf defaultRunOpt
            { dbFile = dbPath opts
            , nThread = 4
            , runMode = Review $ T.pack pid
            , configuration = fromMaybe [] $ configFile opts }

        Write opts pid input -> runWorkflow wf defaultRunOpt
            { dbFile = dbPath opts
            , nThread = 4
            , runMode = Replace (T.pack pid) input
            , configuration = fromMaybe [] $ configFile opts }

        Delete opts pid -> bracket (openDB $ dbPath opts) closeDB
            (delRecord $ T.pack pid)

        Call opts pid inputFl outputFl -> runWorkflow wf defaultRunOpt
            { dbFile = dbPath opts
            , nThread = 4
            , runMode = Slave (T.pack pid) inputFl outputFl
            , configuration = fromMaybe [] $ configFile opts }

        Recover _ _  -> undefined
        DumpDB _ _   -> undefined

                {-
recoverExe (Recover opts dir) (Workflow _ ft _) = do
    fls <- shelly $ lsT $ fromText $ T.pack dir
    shelly $ rm_f $ fromText $ T.pack $ dbPath opts
    db <- openDB $ dbPath opts
    forM_ fls $ \fl -> do
        let pid = snd $ T.breakOnEnd "/" fl
        case M.lookup (T.unpack pid) ft of
            Just (DynFunction fn) -> do
                printf "Recovering node: %s.\n" pid
                c <- B.readFile $ T.unpack fl
                dat <- return (readYaml c) `asTypeOf` fn undefined
                saveData pid dat db
            Nothing -> printf "Cannot identify node: %s. Skipped.\n" pid
            -}

               {-
dumpDBExe (DumpDB opts dir) (Workflow _ ft _) = do
    shelly $ mkdir_p $ fromText $ T.pack dir
    db <- openDB $ dbPath opts
    nodes <- getKeys db
    forM_ nodes $ \pid -> do
        let fl = dir ++ "/" ++ T.unpack pid
        case M.lookup (T.unpack pid) ft of
            Just (DynFunction fn) -> do
                printf "Saving node: %s.\n" pid
                dat <- readData pid db `asTypeOf` fn undefined
                B.writeFile fl $ showYaml dat
            Nothing -> return ()
            -}

runWorkflow :: (Default config, FromJSON config)
            => Workflow config -> RunOpt -> IO ()
runWorkflow (Workflow gr pids wf) opts =
    bracket (mkConnection opts) cleanUp $ \(db, logS) -> do
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

        availableThreads <- newEmptyMVar
        _ <- forkIO $ replicateM_ (nThread opts) $ putMVar availableThreads ()

        let initState = WorkflowState db pidStateMap availableThreads
                (runOnRemote opts) logS

        config <- case configuration opts of
            [] -> return def
            fls -> do
                r <- decodeEither . B.unlines <$> mapM B.readFile fls
                case r of
                    Left err -> error err
                    Right x  -> return x

        result <- runReaderT (runExceptT $ runReaderT (wf ()) initState) config
        case result of
            Right _ -> return ()
            Left (pid, ex) -> sendLog logS $ Error $ printf "\"%s\" failed. The error was: %s."
                pid (displayException ex)

mkConnection :: RunOpt -> IO (WorkflowDB, Maybe Socket)
mkConnection opts = do
    db <- openDB $ dbFile opts
    logS <- case logServerAddr opts of
        Just addr -> do
            sock <- socket AF_UNIX Stream defaultProtocol
            connect sock $ SockAddrUnix addr
            connected <- isConnected sock
            if connected
                then return $ Just sock
                else error "Could not connect to socket!"
        Nothing -> return Nothing
    return (db, logS)

cleanUp :: (WorkflowDB, Maybe Socket) -> IO ()
cleanUp (db, sock) = do
    sendLog sock Exit
    case sock of
        Just s -> close s
        _      -> return ()
    closeDB db
