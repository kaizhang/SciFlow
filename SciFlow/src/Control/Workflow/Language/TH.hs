{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}

module Control.Workflow.Language.TH (build) where

import Control.Arrow.Free (mapA, effect)
import Control.Arrow (arr, (>>>))
import qualified Data.Text as T
import           Language.Haskell.TH
import Instances.TH.Lift ()
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import qualified Data.Graph.Inductive as G
import Control.Monad.State.Lazy (StateT, get, put, lift, execStateT, execState)

import Control.Workflow.Language
import Control.Workflow.Types
import Control.Workflow.Interpreter.FunctionTable (mkFunTable)
import Control.Workflow.Language.TH.Internal

-- | Generate template haskell codes to build the workflow.
build :: String   -- ^ The name of the compiled workflow.
      -> TypeQ    -- ^ The workflow signature.
      -> Builder ()  -- ^ Worflow builder.
      -> Q [Dec]
build name sig builder = compile name sig wf
  where
    wf = addSource $ execState builder $ Workflow M.empty M.empty
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
        main <- compileWorkflow wf
        return [ValD (VarP nm) (NormalB main) []]
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
    return (fst $ M.lookupDefault (errMsg x) x funDefs, M.elems funDefs)
  where
    define :: T.Text
           -> StateT (M.HashMap T.Text FunDef) Q ()
    define nid = do
        mapM_ define ps
        funDefs <- get 
        let parentNames = flip map ps $ \p -> fst $ M.lookupDefault (errMsg p) p funDefs
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
mkJob _ (UNode fun) = [| ustep $fun |]
{-# INLINE mkJob #-}


compileWorkflow :: Workflow -> ExpQ
compileWorkflow wf = 
    let (functions, _, nOutput) = foldl processNode ([], M.empty, 0) $
            flip map (G.topsort' gr) $ \(nid, nd) -> (nid, mkJob nid nd) 
        sink = [| arr $ const () |]
     in linkFunctions $ reverse $ sink : functions
  where
    nodeToParents :: M.HashMap T.Text ([T.Text], Maybe Int)
    nodeToParents = M.fromList $ flip map nodes $ \(_, (x, _)) ->
        let i = M.lookupDefault undefined x nodeToId
            degree = case G.outdeg gr i of
                0 -> Nothing
                d -> Just d
         in (x, (M.lookupDefault [] x $ _parents wf, degree))
    processNode (acc, nodeToPos, nVar) (nid, f) = case M.lookupDefault (error "Impossible") nid nodeToParents of
        ([], Nothing) -> error "singleton"
        ([], Just nChildren) ->
            let nOutput = nChildren
                oIdx = [0 .. nOutput - 1]
                nodeToPos' = M.insert nid oIdx nodeToPos
                fun = [| $f >>> $(replicateOutput 1 nChildren) |]
            in (fun:acc, nodeToPos', nOutput)
        (parents, Nothing) -> 
            let pos = map (\x -> head $ M.lookupDefault (error $ "Node not found" <> show x <> "for" <> show nid) x nodeToPos) parents
                nOutput = nVar - length pos + 1
                nodeToPos' = M.insert nid [0] $ fmap (map (+1)) $
                    M.mapMaybe (adjustIdx pos) nodeToPos
                fun = selectInput nVar pos f
            in (fun:acc, nodeToPos', nOutput)
        (parents, Just nChildren ) -> 
            let pos = map (\x -> head $ M.lookupDefault (error $ "Node not found" <> show x <> "for" <> show nid) x nodeToPos) parents
                nOutput = nVar - length pos + nChildren
                nodeToPos' = M.insert nid [0..nChildren-1] $ fmap (map (+nChildren)) $
                    M.mapMaybe (adjustIdx pos) nodeToPos
                fun = [| $(selectInput nVar pos f) >>> $(replicateOutput (nVar - length pos + 1) nChildren) |]
            in (fun:acc, nodeToPos', nOutput)
    gr = let edges = flip concatMap (M.toList $ _parents wf) $ \(x, ps) ->
                let x' = M.lookupDefault (error $ show x) x nodeToId
                in flip map ps $ \p -> (M.lookupDefault (error $ show p) p nodeToId, x', ())
          in G.mkGraph nodes edges :: G.Gr (T.Text, Node) ()
    nodes = zip [0..] $ M.toList $ _nodes wf
    nodeToId = M.fromList $ map (\(i, (x, _)) -> (x, i)) nodes
{-# INLINE compileWorkflow #-}

addSource :: Workflow -> Workflow
addSource wf = execState builder wf
  where
    builder = do
        uNode name [| \() -> return () |] 
        mapM_ (\x -> [name] ~> x) sources
    sources = filter (\x -> not $ x `M.member` _parents wf) $ M.keys $ _nodes wf
    name = "SciFlow_Source_Node_2xdj23"
{-# INLINE addSource #-}

adjustIdx :: [Int]  -- ^ positions removed
          -> [Int]    -- ^ Old position in the list
          -> Maybe [Int]    -- ^ new position in the list
adjustIdx pos old = case filter (>=0) (map f old) of
    [] -> Nothing
    x -> Just x
  where
    f x = go 0 pos
      where
        go !acc (p:ps) | x == p = -1 
                       | p < x = go (acc+1) ps
                       | otherwise = go acc ps
        go !acc _ = x - acc
{-# INLINE adjustIdx #-}