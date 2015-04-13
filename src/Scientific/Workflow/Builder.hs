module Scientific.Workflow.Builder where

import Control.Arrow (second)
import Control.Monad.State.Lazy (State, modify)
import qualified Data.HashMap.Strict as M
import qualified Data.Text as T
import Data.Tuple (swap)

data Unit = S String
          | L String String
          | L2 (String,String) String
          | L3 (String,String,String) String

data B = B
    { _nodes :: [(String, String, T.Text)]
    , _links :: [(String, Unit)]
    }

type Builder = State B

node :: String -> String -> T.Text -> Builder ()
node l f anno = modify $ \s -> s{_nodes = (l,f,anno) : _nodes s}

singleton :: String -> Builder ()
singleton t = modify $ \s -> s{_links = (t, S t) : _links s}

link :: String -> String -> Builder ()
link a t = modify $ \s -> s{_links = (t, L a t) : _links s}

(~>) :: String -> String -> Builder ()
(~>) = link

link2 :: (String, String) -> String -> Builder ()
link2 (a,b) t = modify $ \s -> s{_links = (t, L2 (a,b) t) : _links s}

link3 :: (String, String, String) -> String -> Builder ()
link3 (a,b,c) t = modify $ \s -> s{_links = (t, L3 (a,b,c) t) : _links s}

data Graph = Graph
    { _children :: M.HashMap String [String]
    , _parents :: M.HashMap String [String]
    , _vertice :: [String]
    }

children :: String -> Graph -> [String]
children x = M.lookupDefault [] x . _children

parents :: String -> Graph -> [String]
parents x = M.lookupDefault [] x . _parents

leaves :: Graph -> [String]
leaves g = filter (\x -> null $ children x g) $ _vertice g

fromUnits :: [Unit] -> Graph
fromUnits us = Graph cs ps vs'
  where
    cs = M.fromListWith (++) $ map (second return) es'
    ps = M.fromListWith (++) $ map (second return . swap) es'
    vs' = concat vs
    es' = concat es
    (vs,es) = unzip $ map f us
    f (S a) = ([a], [])
    f (L a t) = ([a,t], [(a,t)])
    f (L2 (a,b) t) = ([a,b,t], [(a,t),(b,t)])
    f (L3 (a,b,c) t) = ([a,b,c,t], [(a,t),(b,t),(c,t)])
