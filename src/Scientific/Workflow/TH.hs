{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module Scientific.Workflow.TH (build) where

import Control.Arrow
import qualified Data.Text as T
import           Language.Haskell.TH
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S
import Data.Store (Store, encode, decodeEx)
import Control.Funflow.ContentHashable (contentHash, ContentHashable)
import Control.Funflow
import Control.Monad.Identity (Identity(..))
import Control.Monad.State.Lazy (StateT, get, put, lift, execStateT, execState)

import Scientific.Workflow.Types

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
    -- the function signature
    wf_signature <- wfName `sigD` sig

    -- step function definitions
    res <- mapM (mkDefs wf) $ getSinks wf
    let decs = concatMap snd res

    -- main definition
    main <- link (map fst res) [| step $ const () |]

    return [wf_signature, ValD (VarP wfName) (NormalB main) decs]
  where
    wfName = mkName name

-- Create function definitions for the target node and its ancestors.
-- Return the function name of the target node and all relevant function
-- definitions.
mkDefs :: Workflow
       -> T.Text
       -> Q (String, [Dec])
mkDefs wf x = do
    (idToName, decs) <- execStateT (define x) (M.empty, [])
    return (M.lookupDefault undefined x idToName, decs)
  where
    define :: T.Text
           -> StateT (M.HashMap T.Text String, [Dec]) Q ()
    define nd = do
        mapM_ define ps
        (idToName, decs) <- get 
        let parentNames = map (flip (M.lookupDefault undefined) idToName) ps
        e <- lift $ link parentNames [| mkJob nd $ndFun |]
        let dec = ValD (VarP $ mkName ndName) (NormalB e) []
        put (M.insert nd ndName idToName, dec:decs)
        return ()
      where
        ndFun = _node_function $ M.lookupDefault undefined nd $ _nodes wf
        ps = M.lookupDefault [] nd $ _parents wf
        ndName = T.unpack $ T.intercalate "_" $ "f":nd:ps
{-# INLINE mkDefs #-}

link :: [String]  -- a list of parents
        -> ExpQ      -- child
        -> ExpQ
link xs x = case xs of
    [] -> x
    [s] -> [| $(varE $ mkName s) >>> $x |]
    [s1,s2] -> [| $(varE $ mkName s1) &&&
        $(varE $ mkName s2) >>> $x |]
    [s1,s2,s3] -> [| tri $(varE $ mkName s1)
        $(varE $ mkName s2) 
        $(varE $ mkName s3)
        >>> $x |]
{-# INLINE link #-}

tri :: Arrow a => a b c1 -> a b c2 -> a b c3 -> a b (c1, c2, c3)
tri f g h = arr (\x -> (x,(x,x))) >>> f *** (g *** h) >>> arr (\(x,(y,z)) -> (x,y,z))
      
-- | Get all the sinks, i.e., nodes with no children.
getSinks :: Workflow -> [T.Text]
getSinks wf = filter (\x -> not $ S.member x ps) $ M.keys $ _nodes wf
  where
    ps = S.fromList $ concat $ M.elems $ _parents wf
{-# INLINE getSinks #-}

mkJob ::  (Store o, ArrowFlow (Job m) ex arr, ContentHashable Identity i)
      => T.Text -> (i -> m o) -> arr i o
mkJob n f = wrap' prop (Job n (JobConfig Nothing Nothing) f)
  where
    prop = Properties
        { name = Just n
        , cache = cacher
        , mdpolicy = Nothing }
    cacher = Cache
        { cacherKey = \_ i -> runIdentity $ contentHash (n, i)
        , cacherStoreValue = encode
        , cacherReadValue = decodeEx }
{-# INLINE mkJob #-}