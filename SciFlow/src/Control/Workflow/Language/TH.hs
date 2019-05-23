{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Control.Workflow.Language.TH (build) where

import Control.Arrow.Free (mapA, effect)
import Control.Arrow (arr)
import qualified Data.Text as T
import           Language.Haskell.TH
import Instances.TH.Lift ()
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import Control.Monad.State.Lazy (StateT, get, put, lift, execStateT, execState)

import Control.Workflow.Language
import Control.Workflow.Types
import Control.Workflow.Interpreter.FunctionTable (mkFunTable)
import Control.Workflow.Language.TH.Internal

-- | Generate template haskell codes to define the workflow.
build :: String   -- ^ The name of the compiled workflow
      -> TypeQ    -- ^ The function signature
      -> Builder ()
      -> Q [Dec]
build name sig builder = compile name sig wf
  where
    wf = execState builder $ Workflow M.empty M.empty
{-# INLINE build #-}

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
        main <- link (map fst res) [| arr $ const () |]

        return [ValD (VarP nm) (NormalB main) funDecs]
{-# INLINE compile #-}

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
    define nid = do
        mapM_ define ps
        funDefs <- get 
        let parentNames = map (fst . flip (M.lookupDefault undefined) funDefs) ps
        e <- lift $ link parentNames $ mkJob nid $
            M.lookupDefault (errMsg nid) nid $ _nodes wf
        let dec = (ndName, ValD (VarP $ mkName ndName) (NormalB e) [])
        put $ M.insert nid dec funDefs
      where
        ps = M.lookupDefault [] nid $ _parents wf
        ndName = T.unpack $ "f_" <> nid
        errMsg = error . ("Node not found: " ++) .  T.unpack
{-# INLINE mkDefs #-}
     
-- | Get all the sinks, i.e., nodes with no children.
getSinks :: Workflow -> [T.Text]
getSinks wf = filter (\x -> not $ S.member x ps) $ M.keys $ _nodes wf
  where
    ps = S.fromList $ concat $ M.elems $ _parents wf
{-# INLINE getSinks #-}

mkJob :: T.Text -> Node -> ExpQ
mkJob nid Node{..}
    | _node_parallel = [| step $ Job
        { _job_name = nid
        , _job_descr = _node_doc
        , _job_resource = _node_job_resource
        , _job_parallel = True
        , _job_action = mapA $ effect $ Action $_node_function 
        } |]
    | otherwise = [| step $ Job
        { _job_name = nid
        , _job_descr = _node_doc
        , _job_resource = _node_job_resource
        , _job_parallel = False
        , _job_action = effect $ Action $_node_function 
        } |]
{-# INLINE mkJob #-}