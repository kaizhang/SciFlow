{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module Scientific.Workflow.Main
    ( defaultMain
    , defaultMainWith
    , defaultMainOpts
    , MainOpts(..)
    ) where

import           Control.Monad                     (forM_)
import qualified Data.ByteString.Char8             as B
import           Data.Graph.Inductive.Graph        (nmap)
import           Data.Graph.Inductive.PatriciaTree (Gr)
import qualified Data.Map                          as M
import qualified Data.Text                         as T
import qualified Data.Text.Lazy.IO                 as T
import           DRMAA                             (withSGESession)
import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift          as T
import           Options.Applicative               hiding (runParser)
import           Shelly                            (fromText, lsT, mkdir_p,
                                                    rm_f, shelly)
import           Text.Printf                       (printf)

import           Scientific.Workflow
import           Scientific.Workflow.DB
import           Scientific.Workflow.Visualize

import           Data.Version                      (showVersion)
import           Paths_SciFlow                     (version)


data CMD = Run GlobalOpts Int Bool
         | View
         | Cat GlobalOpts String
         | Write GlobalOpts String FilePath
         | Delete GlobalOpts String
         | Recover GlobalOpts FilePath
         | DumpDB GlobalOpts FilePath
         | Call String String String

data GlobalOpts = GlobalOpts
    { dbPath :: FilePath }

globalParser :: Parser GlobalOpts
globalParser = GlobalOpts
           <$> strOption
               ( long "db-path"
              <> value "sciflow.db"
              <> metavar "DB_PATH" )

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
runExe (Run opts n r) wf
    | r = withSGESession $ runWorkflow wf $ RunOpt (dbPath opts) n True
    | otherwise = runWorkflow wf $ RunOpt (dbPath opts) n False
runExe _ _ = undefined
{-# INLINE runExe #-}

viewParser :: Parser CMD
viewParser = pure View
viewExe = T.putStrLn . renderBuilder
{-# INLINE viewExe #-}

catParser :: Parser CMD
catParser = Cat
        <$> globalParser
        <*> strArgument
            (metavar "NODE_ID")
catExe (Cat opts pid) (Workflow _ ft _) = do
    db <- openDB $ dbPath opts
    case M.lookup pid ft of
        Just (Closure fn) -> do
            dat <- head [readData (T.pack pid) db, fn undefined]
            B.putStr $ showYaml dat
        Nothing -> return ()
catExe _ _ = undefined
{-# INLINE catExe #-}

writeParser :: Parser CMD
writeParser = Write
          <$> globalParser
          <*> strArgument
              (metavar "NODE_ID")
          <*> strArgument
              (metavar "INPUT_FILE")
writeExe (Write opts pid input) (Workflow _ ft _) = do
    db <- openDB $ dbPath opts
    c <- B.readFile input
    case M.lookup pid ft of
        Just (Closure fn) -> do
            dat <- head [return $ readYaml c, fn undefined]
            updateData (T.pack pid) dat db
        Nothing -> return ()
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
recoverExe (Recover opts dir) (Workflow _ ft _) = do
    fls <- shelly $ lsT $ fromText $ T.pack dir
    shelly $ rm_f $ fromText $ T.pack $ dbPath opts
    db <- openDB $ dbPath opts
    forM_ fls $ \fl -> do
        let pid = snd $ T.breakOnEnd "/" fl
        case M.lookup (T.unpack pid) ft of
            Just (Closure fn) -> do
                printf "Recovering node: %s.\n" pid
                c <- B.readFile $ T.unpack fl
                dat <- head [return $ readYaml c, fn undefined]
                saveData pid dat db
            Nothing -> printf "Cannot identify node: %s. Skipped.\n" pid
recoverExe _ _ = undefined
{-# INLINE recoverExe #-}

dumpDBParser :: Parser CMD
dumpDBParser = DumpDB
           <$> globalParser
           <*> strArgument
               (metavar "OUTPUT_DIR")
dumpDBExe (DumpDB opts dir) (Workflow _ ft _) = do
    shelly $ mkdir_p $ fromText $ T.pack dir
    db <- openDB $ dbPath opts
    nodes <- getKeys db
    forM_ nodes $ \pid -> do
        let fl = dir ++ "/" ++ T.unpack pid
        case M.lookup (T.unpack pid) ft of
            Just (Closure fn) -> do
                printf "Saving node: %s.\n" pid
                dat <- head [readData pid db, fn undefined]
                B.writeFile fl $ showYaml dat
            Nothing -> return ()
dumpDBExe _ _ = undefined
{-# INLINE dumpDBExe #-}

callParser :: Parser CMD
callParser = Call
         <$> strArgument mempty
         <*> strArgument mempty
         <*> strArgument mempty
callExe (Call pid inputFl outputFl) (Workflow _ ft _) = case M.lookup pid ft of
    Just (Closure fn) -> do
        input <- deserialize <$> B.readFile inputFl
        output <- serialize <$> fn input
        B.writeFile outputFl output
    Nothing -> undefined
callExe _ _ = undefined
{-# INLINE callExe #-}


mainFunc :: Gr (PID, Attribute) Int -> Workflow -> IO ()
mainFunc dag wf = execParser opts >>= execute
  where
    execute cmd@(Run _ _ _) = runExe cmd wf
    execute View = viewExe dag
    execute cmd@(Cat _ _) = catExe cmd wf
    execute cmd@(Write _ _ _) = writeExe cmd wf
    execute cmd@(Delete _ _) = rmExe cmd
    execute cmd@(Recover _ _) = recoverExe cmd wf
    execute cmd@(DumpDB _ _) = dumpDBExe cmd wf
    execute cmd@(Call _ _ _) = callExe cmd wf

    opts = info (helper <*> parser)
            ( fullDesc
           <> header (printf "SciFlow-%s" (showVersion version)) )
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
    { preAction :: Name    -- ^ An action to be execute before the workflow. The
                           -- action should have type: IO () -> IO ().
                            -- ^ i.e., some initialization processes.
    }

T.deriveLift ''MainOpts

defaultMainOpts :: MainOpts
defaultMainOpts = MainOpts 'id

defaultMain :: Builder () -> Q [Dec]
defaultMain = defaultMainWith defaultMainOpts

defaultMainWith :: MainOpts -> Builder () -> Q [Dec]
defaultMainWith opts builder = do
    wf_q <- buildWorkflow wfName builder
    main_q <- [d| main = $(varE $ preAction opts) $ mainFunc dag $(varE $ mkName wfName)
              |]
    return $ wf_q ++ main_q
  where
    wfName = "sciFlowDefaultMain"
    dag = nmap (\(a,(_,b)) -> (a,b)) $ mkDAG builder
{-# INLINE defaultMainWith #-}
