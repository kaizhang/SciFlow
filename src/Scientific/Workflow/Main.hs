{-# LANGUAGE DeriveLift         #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell    #-}

module Scientific.Workflow.Main where

import           Control.Monad.State
import qualified Data.ByteString.Char8             as B
import           Data.Graph.Inductive.Graph        (labEdges, labNodes, mkGraph,
                                                    nmap)
import           Data.Graph.Inductive.PatriciaTree (Gr (..))
import qualified Data.Text                         as T
import qualified Data.Text.Lazy.IO                 as T
import           Language.Haskell.TH
import qualified Language.Haskell.TH.Lift          as T
import           Scientific.Workflow
import           Scientific.Workflow.DB
import           Scientific.Workflow.Visualize
import           System.Environment

deriving instance T.Lift Attribute

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

mainFunc :: Gr (PID, Attribute) Int -> Workflows -> IO ()
mainFunc dag wf = do
    (cmd:args) <- getArgs
    case cmd of
        "run" -> runWorkflow wf $ return ()
        "view" -> T.putStrLn $ renderBuilder dag
        "rm" -> do
            db <- openDB $ _dbPath opt
            delRecord (T.pack $ head args) db
        "read" -> do
            db <- openDB $ _dbPath opt
            dat <- readDataByteString (T.pack $ head args) db
            B.putStrLn dat
  where
    opt = execState (return ()) defaultRunOpt
