{-# LANGUAGE FlexibleInstances #-}
module Scientific.Workflow.Builder where

import           Control.Arrow             (second)
import           Control.Monad.State.Lazy  (State, foldM_, modify)
import Data.Array (listArray)
import qualified Data.HashMap.Strict       as M
import qualified Data.Text                 as T
import           Data.Tuple                (swap)
import qualified Data.Graph as G
import           Data.List                 (nub)
import           Language.Haskell.TH

import           Scientific.Workflow.Types

-- | Factors are small subgraphs/units of workflows. Each factor is associated
-- with multiple inputs and a single output
data Factor = S String
            | L String String
            | L2 (String,String) String
            | L3 (String,String,String) String
            | L4 (String,String,String,String) String
            | L5 (String,String,String,String,String) String
            | L6 (String,String,String,String,String,String) String
    deriving (Show)

-- | State of Builder Monad, storing workflow structure
data B = B
    { _nodes :: [(String, ExpQ, T.Text)]
    , _links :: [(String, Factor)]
    }

type Builder = State B

-- | Objects that can be converted to ExpQ
class ToExpQ a where
    toExpQ :: a -> ExpQ

instance ToExpQ Name where
    toExpQ = varE

instance ToExpQ ExpQ where
    toExpQ = id

-- | Declare a computational node
node :: ToExpQ a => String -> a -> T.Text -> Builder ()
node l f anno = modify $ \s -> s{_nodes = (l, toExpQ f, anno) : _nodes s}

-- | many-to-one generalized link function
link :: [String] -> String -> Builder ()
link [] t = singleton t
link [a] t = link1 a t
link [a,b] t = link2 (a,b) t
link [a,b,c] t = link3 (a,b,c) t
link [a,b,c,d] t = link4 (a,b,c,d) t
link [a,b,c,d,e] t = link5 (a,b,c,d,e) t
link [a,b,c,d,e,f] t = link6 (a,b,c,d,e,f) t
link _ _ = error "I can't have so many links, yet!"

-- | (~>) = link.
(~>) :: [String] -> String -> Builder ()
(~>) = link

-- | singleton
singleton :: String -> Builder ()
singleton t = modify $ \s -> s{_links = (t, S t) : _links s}

-- | Declare a path.
path :: [String] -> Builder ()
path ns = foldM_ f (head ns) $ tail ns
  where
    f a t = link1 a t >> return t

-- | one-to-one link
link1 :: String -> String -> Builder ()
link1 a t = modify $ \s -> s{_links = (t, L a t) : _links s}

-- | two-to-one link
link2 :: (String, String) -> String -> Builder ()
link2 (a,b) t = modify $ \s -> s{_links = (t, L2 (a,b) t) : _links s}

-- | tree-to-one link
link3 :: (String, String, String) -> String -> Builder ()
link3 (a,b,c) t = modify $ \s -> s{_links = (t, L3 (a,b,c) t) : _links s}

link4 :: (String, String, String, String) -> String -> Builder ()
link4 (a,b,c,d) t = modify $ \s -> s{_links = (t, L4 (a,b,c,d) t) : _links s}

link5 :: (String, String, String, String, String) -> String -> Builder ()
link5 (a,b,c,d,e) t = modify $ \s -> s{_links = (t, L5 (a,b,c,d,e) t) : _links s}

link6 :: (String, String, String, String, String, String) -> String -> Builder ()
link6 (a,b,c,d,e,f) t = modify $ \s -> s{_links = (t, L6 (a,b,c,d,e,f) t) : _links s}


data Graph = Graph
    { _children :: !(M.HashMap String [String])
    , _parents  :: !(M.HashMap String [String])
    , _vertice  :: ![ID]
    }

children :: ID -> Graph -> [ID]
children x = M.lookupDefault [] x . _children
{-# INLINE children #-}

parents :: ID -> Graph -> [ID]
parents x = M.lookupDefault [] x . _parents
{-# INLINE parents #-}

leaves :: Graph -> [ID]
leaves g = filter (\x -> null $ children x g) $ _vertice g
{-# INLINE leaves #-}

topSort :: Graph -> [ID]
topSort gr = map (flip (M.lookupDefault undefined) idToLab) $ G.topSort gr'
  where
    gr' = listArray (0,n-1) $ map (map (flip (M.lookupDefault undefined) labToId) . flip children gr) $ _vertice gr
    n = M.size labToId
    labToId = M.fromList $ zip (_vertice gr) [0..]
    idToLab = M.fromList $ zip [0..] (_vertice gr)
{-# INLINE topSort #-}

reachable :: ID -> Graph -> [ID]
reachable x gr = go $ parents x gr
  where
    go xs = xs ++ concatMap (flip parents gr) xs
{-# INLINE reachable #-}

fromFactors :: [Factor] -> Graph
fromFactors us = Graph cs ps vs'
  where
    cs = M.fromListWith (++) $ map (second return) es'
    ps = M.fromListWith (++) $ map (second return . swap) es'
    vs' = nub $ concat vs
    es' = concat es
    (vs,es) = unzip $ map fn us
    fn (S a) = ([a], [])
    fn (L a t) = ([a,t], [(a,t)])
    fn (L2 (a,b) t) = ([a,b,t], [(a,t),(b,t)])
    fn (L3 (a,b,c) t) = ([a,b,c,t], [(a,t),(b,t),(c,t)])
    fn (L4 (a,b,c,d) t) = ([a,b,c,d,t], [(a,t),(b,t),(c,t),(d,t)])
    fn (L5 (a,b,c,d,e) t) = ([a,b,c,d,e,t], [(a,t),(b,t),(c,t),(d,t),(e,t)])
    fn (L6 (a,b,c,d,e,f) t) = ([a,b,c,d,e,f,t], [(a,t),(b,t),(c,t),(d,t),(e,t),(f,t)])
{-# INLINE fromFactors #-}
