{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TemplateHaskell   #-}

module Scientific.Workflow.Main where

import           Control.Monad.State
import qualified Data.ByteString.Char8             as B
import           Data.Graph.Inductive.Graph        (labEdges, labNodes, mkGraph,
                                                    nmap)
import           Data.Graph.Inductive.PatriciaTree (Gr (..))
import qualified Data.Map                          as M
import           Data.Maybe                        (fromJust)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy.IO                 as T
import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift          as T
import           System.Environment
import Shelly

import           Scientific.Workflow
import           Scientific.Workflow.DB
import           Scientific.Workflow.Visualize

T.deriveLift ''Attribute

instance T.Lift (Gr (PID, Attribute) Int) where
  lift gr = [| uncurry mkGraph $(T.lift (labNodes gr, labEdges gr)) |]

defaultMain :: Builder () -> Q [Dec]
defaultMain builder = do
    wf_q <- buildWorkflow wfName builder
    main_q <- [d| main = mainFunc dag $(varE $ mkName wfName) |]
    return $ wf_q ++ main_q
  where
    wfName = "sciFlowDefaultMain"
    dag = nmap (\(a,(_,b)) -> (a,b)) $ mkDAG builder

mainFunc :: Gr (PID, Attribute) Int -> Workflow -> IO ()
mainFunc dag wf@(Workflow _ ft _) = do
    (cmd:args) <- getArgs
    case cmd of
        "run" -> runWorkflow wf $ return ()
        "view" -> T.putStrLn $ renderBuilder dag
        "rm" -> do
            db <- openDB $ _dbPath opt
            delRecord (T.pack $ head args) db
        "read" -> do
            db <- openDB $ _dbPath opt
            case M.lookup (head args) ft of
                Just (Closure fn) -> do
                    proxy <- fn undefined
                    dat <- readData (T.pack $ head args) db
                    B.putStr $ showYaml $ head [dat, proxy]
                Nothing -> return ()
        "write" -> do
            db <- openDB $ _dbPath opt
            c <- B.readFile (args!!1)
            case M.lookup (head args) ft of
                Just (Closure fn) -> do
                    proxy <- fn undefined
                    updateData (T.pack $ args!!0) (head [readYaml c, proxy]) db
                Nothing -> return ()
        {-
        "recover" -> do
            db <- openDB $ _dbPath opt
            lsT $ fromText $ T.pack $ head args
            -}
  where
    opt = execState (return ()) defaultRunOpt
