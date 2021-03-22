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
import qualified Data.Graph.Inductive as G
import Control.Monad.State.Lazy (execState)
import Data.Hashable (hash)
import Data.Maybe (fromJust)

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
    d3 <- [d| $(varP $ mkName name) = SciFlow $(varE wfName) $(varE tableName) $ G.mkGraph nodes edges |]
    return $ d1 ++ d2 ++ (wf_signature:d3)
  where
    nodes =
        let mkNodeLabel k (UNode _) = NodeLabel k "" False True
            mkNodeLabel k Node{..} = NodeLabel k _node_doc _node_parallel False
         in flip map (M.toList $ _nodes wf) $ \(k, nd) -> (hash k, mkNodeLabel k nd)
    edges = flip concatMap (M.toList $ _parents wf) $ \(x, ps) ->
        flip map ps $ \p -> (hash p, hash x, ())
    tableName = mkName $ name ++ "__Table"
    wfName = mkName $ name ++ "__Flow"
    defFlow nm = do
        main <- compileWorkflow wf
        return [ValD (VarP nm) (NormalB main) []]
{-# INLINE compile #-}

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
mkJob nid (UNode fun) = [| ustep nid $fun |]
{-# INLINE mkJob #-}


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


compileWorkflow :: Workflow -> ExpQ
compileWorkflow wf = 
    let (functions, _, _) = foldl processNodeGroup ([], M.empty, 0) $ groupSortNodes wf
        sink = [| arr $ const () |]
     in linkFunctions $ reverse $ sink : functions
  where
    nodeToParents :: M.HashMap T.Text ([T.Text], Int)
    nodeToParents = M.fromList $ flip map nodes $ \(_, (x, _)) ->
        let i = M.lookupDefault undefined x nodeToId
            degree = case G.outdeg gr i of
                0 -> 1
                d -> d
         in (x, (M.lookupDefault [] x $ _parents wf, degree))
    processNodeGroup (acc, nodeToPos, nVar) nodes =
        case (map (\x -> M.lookupDefault (error "Impossible") (fst x) nodeToParents) nodes) of
            -- source node
            [([], n)] ->
                let oIdx = [0 .. n - 1]
                    (nid, f) = head nodes
                    nodeToPos' = M.insert nid oIdx nodeToPos
                    fun = [| $f >>> $(replicateOutput 1 n) |]
                in (fun:acc, nodeToPos', n)
            parents -> 
                let inputPos =
                        let computeIdx count (x:xs) = let (x', count') = go ([], count) x in x' : computeIdx count' xs
                              where
                                go (acc, m) (y:ys) = case M.lookup y m of
                                    Nothing -> go (acc ++ [(y, 0)], M.insert y 1 m) ys
                                    Just c -> go (acc ++ [(y, c)], M.insert y (c+1) m) ys
                                go acc _ = acc
                            computeIdx _ _ = []
                            lookupP (p, i) = M.lookupDefault errMsg p nodeToPos !! i
                              where
                                errMsg = error $ unlines $
                                    ("Node not found: " <> show p) :
                                    show (map fst nodes) :
                                    map show (M.toList nodeToPos)
                                    --map show (M.toList nodeToParents)
                        in map (map lookupP) $ computeIdx M.empty $ map fst parents
                    nInput = map length inputPos
                    nOutput = map snd parents
                    nodeToPos' =
                        let outputPos = let i = scanl1 (+) nOutput
                                         in zipWith (\a b -> [a .. b-1]) (0:i) i
                            m = fmap (map (+(sum nOutput))) $ 
                                M.mapMaybe (adjustIdx $ concat inputPos) nodeToPos
                         in foldl (\x (k, v) -> M.insert k v x) m $ zip (map fst nodes) outputPos
                    fun =
                        let combinedF = combineArrows $
                                flip map (zip3 nodes nInput nOutput) $ \((_, f), ni, no) ->
                                (ni, [| $f >>> $(replicateOutput 1 no) |], no)
                        in selectInput nVar (sum nOutput) (concat inputPos) combinedF
                    nVar' = nVar - (sum nInput) + (sum nOutput)
                in (fun:acc, nodeToPos', nVar')
    gr = let edges = flip concatMap (M.toList $ _parents wf) $ \(x, ps) ->
                let x' = M.lookupDefault (error $ show x) x nodeToId
                in flip map ps $ \p -> (M.lookupDefault (error $ show p) p nodeToId, x', ())
          in G.mkGraph nodes edges :: G.Gr (T.Text, Node) ()
    nodes = zip [0..] $ M.toList $ _nodes wf
    nodeToId = M.fromList $ map (\(i, (x, _)) -> (x, i)) nodes
{-# INLINE compileWorkflow #-}

groupSortNodes :: Workflow -> [[(T.Text, ExpQ)]]
groupSortNodes wf = go [] $ G.topsort' gr
  where
    go acc [] = [acc]
    go [] (x:xs) = go [x] xs
    go acc (x:xs) | any (x `isChildOf`) acc = acc : go [x] xs
                  | otherwise = go (acc <> [x]) xs
    isChildOf x y = gr `G.hasEdge` (hash $ fst y, hash $ fst x)
    gr = let edges = flip concatMap (M.toList $ _parents wf) $ \(x, ps) ->
                flip map ps $ \p -> (hash p, hash x, ())
          in G.mkGraph nodes edges :: G.Gr (T.Text, ExpQ) ()
    nodes = map (\(k, x) -> (hash k, (k, mkJob k x))) $ M.toList $ _nodes wf
{-# INLINE groupSortNodes #-}