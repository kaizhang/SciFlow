{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Control.Workflow.Language.TH (build) where

import Control.Arrow
import Control.Arrow.Free (Choice, mapA)
import Control.Monad.Reader
import Data.Binary (Binary)
import qualified Data.Text as T
import           Language.Haskell.TH
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import Control.Funflow.ContentHashable (contentHash, ContentHashable)
import Control.Monad.Identity (Identity(..))
import Control.Monad.State.Lazy (StateT, get, put, lift, execStateT, execState)

import Control.Workflow.Types
import Control.Workflow.Interpreter.FunctionTable (mkFunTable)

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
            then [| mapA $ mkJob nd $(varE _node_function) |]
            else [| mkJob nd $(varE _node_function) |]
        let dec = (ndName, ValD (VarP $ mkName ndName) (NormalB e) [])
        put $ M.insert nd dec funDefs
      where
        Node{..} = M.lookupDefault undefined nd $ _nodes wf
        ps = M.lookupDefault [] nd $ _parents wf
        ndName = T.unpack $ "f_" <> nd
{-# INLINE mkDefs #-}

link :: [String]  -- a list of parents
     -> ExpQ      -- child
     -> ExpQ
link xs x = case xs of
    [] -> [| (ustep $ \() -> return ()) >>> $x |]
    [s] -> [| $(varE $ mkName s) >>> $x |]
    [s1,s2] -> [| $(varE $ mkName s1) &&&
        $(varE $ mkName s2) >>> $x |]
    [s1,s2,s3] -> [| tri $(varE $ mkName s1)
        $(varE $ mkName s2) 
        $(varE $ mkName s3)
        >>> $x |]
    _ -> error "NO IMPLEMENTATION!"
{-# INLINE link #-}

tri :: Arrow arr => arr i o1 -> arr i o2 -> arr i o3 -> arr i (o1, o2, o3)
tri f g h = arr (\x -> (x,(x,x))) >>> f *** (g *** h) >>> arr (\(x,(y,z)) -> (x,y,z))
{-# INLINE tri #-}
      
-- | Get all the sinks, i.e., nodes with no children.
getSinks :: Workflow -> [T.Text]
getSinks wf = filter (\x -> not $ S.member x ps) $ M.keys $ _nodes wf
  where
    ps = S.fromList $ concat $ M.elems $ _parents wf
{-# INLINE getSinks #-}

mkJob :: (Binary i, Binary o, ContentHashable Identity i)
      => T.Text -> (i -> ReaderT env IO o) -> Choice (Flow env) i o
mkJob n f = step job
  where
    job = Job
        { _job_name = n 
        , _job_config = JobConfig Nothing Nothing
        , _job_action = f
        , _job_cache = (\i -> runIdentity $ contentHash (n, i)) }
{-# INLINE mkJob #-}