module Scientific.Workflow.Builder where

import Control.Arrow (second)
import Control.Monad.State.Lazy (State, modify)
import qualified Data.HashMap.Strict as M
import qualified Data.Text as T
import Data.Tuple (swap)

data Unit = Link String String
          | Link2 (String,String) String
          | Link3 (String,String,String) String

data B = B
    { _nodes :: [(String, String, T.Text)]
    , _links :: [(String, Unit)]
    }

type Builder = State B

node :: String -> String -> T.Text -> Builder ()
node l f anno = modify $ \s -> s{_nodes = (l,f,anno) : _nodes s}

link :: String -> String -> Builder ()
link a b = modify $ \s -> s{_links = (b, Link a b) : _links s}

(~>) :: String -> String -> Builder ()
(~>) = link

link2 :: (String, String) -> String -> Builder ()
link2 (a,b) c = modify $ \s -> s{_links = (c, Link2 (a,b) c) : _links s}

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
    f (Link a b) = ([a,b], [(a,b)])
    f (Link2 (a,b) c) = ([a,b,c], [(a,c),(b,c)])
    f (Link3 (a,b,c) d) = ([a,b,c,d], [(a,d),(b,d),(c,d)])
