{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Control.Workflow.Language.TH (build) where

import Control.Arrow.Free (Free, mapA, effect)
import Control.Monad.Reader
import Data.Binary (Binary)
import Data.Typeable (Typeable)
import qualified Data.Text as T
import           Language.Haskell.TH
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import Control.Funflow.ContentHashable (contentHash, ContentHashable)
import Control.Monad.Identity (Identity(..))
import Control.Monad.State.Lazy (StateT, get, put, lift, execStateT, execState)

import Control.Workflow.Language
import Control.Workflow.Types
import Control.Workflow.Interpreter.FunctionTable (mkFunTable)
import Control.Workflow.Language.TH.Internal

build :: String
      -> TypeQ
      -> Builder ()
      -> Q [Dec]
build name sig builder = compile name sig wf
  where
    wf = execState builder $ Workflow M.empty M.empty

-- Generate codes from a DAG. This function will create functions defined in
-- the builder. These pieces will be assembled to form a function that will
-- execute each individual function in a correct order.
compile :: String     -- ^ The name of the compiled workflow
        -> TypeQ      -- ^ The function signature
        -> Workflow
        -> Q [Dec]
compile name sig wf = do
    d1 <- defFlow wfName
    d2 <- mkFunTable (name ++ "__Table") (name ++ "__Flow")
    -- the function signature
    wf_signature <- (mkName name) `sigD` sig
    d3 <- [d| $(varP $ mkName name) = SciFlow $(varE wfName) $(varE tableName) |]
    return $ d1 ++ d2 ++ (wf_signature:d3)
  where
    tableName = mkName $ name ++ "__Table"
    wfName = mkName $ name ++ "__Flow"
    defFlow nm = do
        -- step function definitions
        res <- mapM (mkDefs wf) $ getSinks wf
        let funDecs = M.elems $ M.fromList $ concatMap snd res

        -- main definition
        main <- link (map fst res) [| ustep $ const $ return () |]

        return [ValD (VarP nm) (NormalB main) funDecs]

type FunDef = (String, Dec)

-- Create function definitions for the target node and its ancestors.
-- Return the function name of the target node and all relevant function
-- definitions.
mkDefs :: Workflow
       -> T.Text
       -> Q (String, [FunDef])
mkDefs wf x = do
    funDefs <- execStateT (define x) M.empty
    return (fst $ M.lookupDefault undefined x funDefs, M.elems funDefs)
  where
    define :: T.Text
           -> StateT (M.HashMap T.Text FunDef) Q ()
    define nd = do
        mapM_ define ps
        funDefs <- get 
        let parentNames = map (fst . flip (M.lookupDefault undefined) funDefs) ps
        e <- lift $ link parentNames $ if _node_parallel
            then [| mkJobP nd $(varE _node_function) |]
            else [| mkJob nd $(varE _node_function) |]
        let dec = (ndName, ValD (VarP $ mkName ndName) (NormalB e) [])
        put $ M.insert nd dec funDefs
      where
        Node{..} = M.lookupDefault (errMsg nd) nd $ _nodes wf
        ps = M.lookupDefault [] nd $ _parents wf
        ndName = T.unpack $ "f_" <> nd
        errMsg = error . ("Node not found: " ++) .  T.unpack
{-# INLINE mkDefs #-}
     
-- | Get all the sinks, i.e., nodes with no children.
getSinks :: Workflow -> [T.Text]
getSinks wf = filter (\x -> not $ S.member x ps) $ M.keys $ _nodes wf
  where
    ps = S.fromList $ concat $ M.elems $ _parents wf
{-# INLINE getSinks #-}

mkJob :: (Binary i, Binary o, ContentHashable Identity i)
      => T.Text -> (i -> ReaderT env IO o) -> Free (Flow env) i o
mkJob n f = step job
  where
    job = Job
        { _job_name = n 
        , _job_config = JobConfig Nothing Nothing
        , _job_parallel = False
        , _job_action = effect $
            Action f (\i -> runIdentity $ contentHash (n, i)) }
{-# INLINE mkJob #-}

mkJobP :: (Binary i, Binary o, ContentHashable Identity i, Typeable i)
       => T.Text -> (i -> ReaderT env IO o) -> Free (Flow env) [i] [o]
mkJobP n f = step job
  where
    job = Job
        { _job_name = n 
        , _job_config = JobConfig Nothing Nothing
        , _job_parallel = True
        , _job_action = mapA $ effect $
            Action f (\i -> runIdentity $ contentHash (n, i)) }
{-# INLINE mkJobP #-}