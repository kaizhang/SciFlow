{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}

module Scientific.Workflow.TH (compile) where

import Control.Arrow
import qualified Data.Text as T
import           Language.Haskell.TH
import qualified Data.HashMap.Strict as M
import qualified Data.HashSet as S

import Scientific.Workflow.Types

-- Generate codes from a DAG. This function will create functions defined in
-- the builder. These pieces will be assembled to form a function that will
-- execute each individual function in a correct order.
-- Lastly, a function table will be created with the name $name$_function_table.
compile :: String   -- ^ The name of the compiled workflow
        -> TypeQ
        -> Workflow
        -> Q [Dec]
compile name sig wf = do
    let wfname = mkName name
    wf_signature <- wfname `sigD` sig

    res <- mapM define $ getSinks wf
    exp <- aux (fst $ unzip res) [| step $ const () |]
    let decs = M.elems $ M.fromList $ concat $ snd $ unzip res

    return [wf_signature, ValD (VarP wfname) (NormalB exp) decs]
  where
    define :: T.Text -> Q (String, [(String, Dec)])
    define x = do
          ds <- mapM define ps
          d <- do
              exp <- aux (map fst ds) e 
              return $ ValD (VarP $ mkName n) (NormalB exp) []
          return (n, (n, d) : concatMap snd ds)
        where
          e = _node_function $ M.lookupDefault undefined x $ _nodes wf
          ps = M.lookupDefault [] x $ _parents wf
          n = T.unpack $ T.intercalate "_" $ "f":x:ps
    aux xs x = case xs of
        [] -> x
        [s] -> [| $(varE $ mkName s) >>> $x |]
        [s1,s2] -> [| $(varE $ mkName s1) &&&
            $(varE $ mkName s2) >>> $x |]
        [s1,s2,s3] -> [| tri $(varE $ mkName s1)
            $(varE $ mkName s2) 
            $(varE $ mkName s3)
            >>> $x |]

tri :: Arrow a => a b c1 -> a b c2 -> a b c3 -> a b (c1, c2, c3)
tri f g h = arr (\x -> (x,(x,x))) >>> f *** (g *** h) >>> arr (\(x,(y,z)) -> (x,y,z))
      
-- | Get all the sinks, i.e., nodes with no children.
getSinks :: Workflow -> [T.Text]
getSinks wf = filter (\x -> not $ S.member x ps) $ M.keys $ _nodes wf
  where
    ps = S.fromList $ concat $ M.elems $ _parents wf
{-# INLINE getSinks #-}