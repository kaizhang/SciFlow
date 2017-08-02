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

import qualified Data.ByteString.Char8             as B
import           Data.Graph.Inductive.Graph        (nmap)
import           Data.Graph.Inductive.PatriciaTree (Gr)
import           Data.List.Split                   (splitOn)
import qualified Data.Map                          as M
import           Data.Semigroup                    ((<>))
import           Data.Serialize                    (encode)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy.IO                 as T

#ifdef DRMAA
import           DRMAA                             (withSession)
#endif

import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift          as T
import           Options.Applicative               hiding (runParser)
import           Shelly                            (fromText, lsT, mkdir_p,
                                                    rm_f, shelly)
import           Text.Printf                       (printf)

import           Data.Version                      (showVersion)
import           Paths_SciFlow                     (version)
import           Scientific.Workflow
import           Scientific.Workflow.Internal.DB
import           Scientific.Workflow.Internal.Builder.Types
import           Scientific.Workflow.Visualize

data CMD = Run GlobalOpts Int Bool (Maybe [String])
         | View Bool
         | Cat GlobalOpts String
         | Write GlobalOpts String FilePath
         | Delete GlobalOpts String
         | Recover GlobalOpts FilePath
         | DumpDB GlobalOpts FilePath
         | Call GlobalOpts String String String

data GlobalOpts = GlobalOpts
    { dbPath     :: FilePath
    , configFile :: Maybe FilePath
    }

globalParser :: Parser GlobalOpts
globalParser = GlobalOpts
           <$> strOption
               ( long "db-path"
              <> value "sciflow.db"
              <> metavar "DB_PATH" )
           <*> (optional . strOption)
               ( long "config"
              <> metavar "CONFIG_PATH" )


runParser :: Parser CMD
runParser = Run
    <$> globalParser
    <*> option auto
        ( short 'N'
       <> value 1
       <> metavar "CORES"
       <> help "The number of concurrent processes." )
    <*> switch
        ( long "remote"
       <> help "Submit jobs to remote machines.")
    <*> (optional . option (maybeReader (Just . splitOn ",")))
        ( long "select"
       <> metavar "SELECTED"
       <> help "Run only selected nodes.")
runExe initialize (Run opts n r s) wf
#ifdef DRMAA
    | r = initialize $ withSession $ runWorkflow wf runOpts
#else
    | r = initialize $ runWorkflow wf runOpts
#endif
    | otherwise = runWorkflow wf runOpts{runOnRemote = False}
  where
    runOpts = defaultRunOpt
        { database = dbPath opts
        , runOnRemote = True
        , nThread = n
        , configuration = configFile opts
        , selected = fmap (map T.pack) s }
runExe _ _ _ = undefined
{-# INLINE runExe #-}

viewParser :: Parser CMD
viewParser = View <$> switch (long "raw")
viewExe (View isRaw) wf | isRaw = B.putStr $ encode wf
                        | otherwise = T.putStrLn $ drawWorkflow wf
{-# INLINE viewExe #-}

catParser :: Parser CMD
catParser = Cat
        <$> globalParser
        <*> strArgument
            (metavar "NODE_ID")
catExe (Cat opts pid) wf = runWorkflow wf defaultRunOpt
    { database = dbPath opts
    , nThread = 10
    , runMode = Review $ T.pack pid
    , configuration = configFile opts }
catExe _ _ = undefined
{-# INLINE catExe #-}

writeParser :: Parser CMD
writeParser = Write
          <$> globalParser
          <*> strArgument
              (metavar "NODE_ID")
          <*> strArgument
              (metavar "INPUT_FILE")
writeExe (Write opts pid input) wf = runWorkflow wf defaultRunOpt
    { database = dbPath opts
    , nThread = 10
    , runMode = Replace (T.pack pid) input
    , configuration = configFile opts }
writeExe _ _ = undefined
{-# INLINE writeExe #-}

rmParser :: Parser CMD
rmParser = Delete
       <$> globalParser
       <*> strArgument
           (metavar "NODE_ID")
rmExe (Delete opts pid) = do
    db <- openDB $ dbPath opts
    delRecord (T.pack pid) db
rmExe _ = undefined
{-# INLINE rmExe #-}

recoverParser :: Parser CMD
recoverParser = Recover
            <$> globalParser
            <*> strArgument
                (metavar "BACKUP")
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
recoverExe _ _ = undefined
{-# INLINE recoverExe #-}

dumpDBParser :: Parser CMD
dumpDBParser = DumpDB
           <$> globalParser
           <*> strArgument
               (metavar "OUTPUT_DIR")
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
dumpDBExe _ _ = undefined
{-# INLINE dumpDBExe #-}

callParser :: Parser CMD
callParser = Call
         <$> globalParser
         <*> strArgument mempty
         <*> strArgument mempty
         <*> strArgument mempty
callExe (Call opts pid inputFl outputFl) wf = runWorkflow wf defaultRunOpt
    { database = dbPath opts
    , nThread = 10
    , runMode = Slave (T.pack pid) inputFl outputFl
    , configuration = configFile opts }
callExe _ _ = undefined
{-# INLINE callExe #-}



mainFunc :: (IO () -> IO ()) -- initialization function
         -> Gr (PID, Attribute) Int -> Workflow
         -> String  -- program header
         -> IO ()
mainFunc initialize dag wf h = execParser opts >>= execute
  where
    execute cmd@(Run _ _ _ _)  = runExe initialize cmd wf
    execute cmd@(View _)       = viewExe cmd dag
    execute cmd@(Cat _ _)      = catExe cmd wf
    execute cmd@(Write _ _ _)  = writeExe cmd wf
    execute cmd@(Delete _ _)   = rmExe cmd
    execute cmd@(Recover _ _)  = recoverExe cmd wf
    execute cmd@(DumpDB _ _)   = dumpDBExe cmd wf
    execute cmd@(Call _ _ _ _) = callExe cmd wf

    opts = info (helper <*> parser) $ fullDesc <> header h
    parser = subparser $ (
        command "run" (info (helper <*> runParser) $
            fullDesc <> progDesc "run workflow")
     <> command "view" (info (helper <*> viewParser) $
            fullDesc <> progDesc "view workflow")
     <> command "cat" (info (helper <*> catParser) $
            fullDesc <> progDesc "display the result of a node")
     <> command "write" (info (helper <*> writeParser) $
            fullDesc <> progDesc "write the result to a node")
     <> command "rm" (info (helper <*> rmParser) $
            fullDesc <> progDesc "delete the result of a node.")
     <> command "recover" (info (helper <*> recoverParser) $
            fullDesc <> progDesc "Recover database from backup.")
     <> command "backup" (info (helper <*> dumpDBParser) $
            fullDesc <> progDesc "Backup database.")
     <> command "execFunc" (info (helper <*> callParser) $
            fullDesc <> progDesc "Do not call this directly.")
     )


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
