{-# LANGUAGE CPP               #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Scientific.Workflow.Main
    ( defaultMain
    , defaultMainOpts
    , mainWith
    , MainOpts(..)
    ) where

import           Control.Exception                 (bracket)
import qualified Data.ByteString.Char8             as B
import           Data.Default.Class                (Default)
import           Data.Graph.Inductive.Graph        (nmap)
import           Data.Graph.Inductive.PatriciaTree (Gr)
import           Data.Maybe                        (fromMaybe)
import           Data.Serialize                    (encode)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy.IO                 as T
import           Data.Yaml                         (FromJSON)

#ifdef DRMAA_ENABLED
import           DRMAA                             (withSession)
#endif

import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift          as T
import           Options.Applicative
import           Text.Printf                       (printf)

import           Data.Version                      (showVersion)
import           Paths_SciFlow                     (version)
import           Scientific.Workflow
import           Scientific.Workflow.Internal.DB
import           Scientific.Workflow.Main.Options  (CMD (..), GlobalOpts (..),
                                                    argsParser)
import           Scientific.Workflow.Visualize

data MainOpts = MainOpts
    { preAction     :: Name    -- ^ An action to be execute before the workflow. The
                           -- action should have type: IO () -> IO ().
                            -- ^ i.e., some initialization processes.
    , programHeader :: String
    }

T.deriveLift ''MainOpts

defaultMainOpts :: MainOpts
defaultMainOpts = MainOpts
    { preAction = 'id
    , programHeader = printf "SciFlow-%s" (showVersion version)
    }

defaultMain :: Builder () -> Q [Dec]
defaultMain = mainWith defaultMainOpts

mainWith :: MainOpts -> Builder () -> Q [Dec]
mainWith opts builder = do
    wf_q <- buildWorkflow wfName builder
    main_q <- [d| main = mainFunc $(varE $ preAction opts) dag
                    $(varE $ mkName wfName) (programHeader opts)
              |]
    return $ wf_q ++ main_q
  where
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
        Run opts n r s ->
            let runOpts = defaultRunOpt
                    { dbFile = dbPath opts
                    , runOnRemote = True
                    , nThread = n
                    , configuration = fromMaybe [] $ configFile opts
                    , selected = fmap (map T.pack) s }
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
