module Scientific.Workflow.Builder where

import Control.Arrow (second)
import Control.Monad.State.Lazy (State, modify)
import qualified Data.HashMap.Strict as M
import qualified Data.Text as T
import Data.Tuple (swap)

data Unit = Singleton !T.Text
          | Link !T.Text !T.Text
          | Link2 !(T.Text, T.Text) !T.Text
          | Link3 !(T.Text, T.Text, T.Text) !T.Text
          | Link4 !(T.Text, T.Text, T.Text, T.Text) !T.Text
          | Link5 !(T.Text, T.Text, T.Text, T.Text, T.Text) !T.Text
          | Link6 !(T.Text, T.Text, T.Text, T.Text, T.Text, T.Text) !T.Text

type Builder = State [(T.Text, Unit)]

link :: T.Text -> T.Text -> Builder ()
link a b = modify ((b, Link a b) :)

link2 :: (T.Text, T.Text) -> T.Text -> Builder ()
link2 (a,b) c = modify ((c, Link2 (a,b) c) :)

data Graph = Graph
    { _children :: M.HashMap T.Text [T.Text]
    , _parents :: M.HashMap T.Text [T.Text]
    , _vertice :: [T.Text]
    }

children :: T.Text -> Graph -> [T.Text]
children x = M.lookupDefault [] x . _children

parents :: T.Text -> Graph -> [T.Text]
parents x = M.lookupDefault [] x . _parents

leaves :: Graph -> [T.Text]
leaves g = filter (\x -> null $ children x g) $ _vertice g

fromUnits :: [Unit] -> Graph
fromUnits us = Graph cs ps vs'
  where
    cs = M.fromListWith (++) $ map (second return) es'
    ps = M.fromListWith (++) $ map (second return . swap) es'
    vs' = concat vs
    es' = concat es
    (vs,es) = unzip $ map f us
    f (Singleton a) = ([a], [])
    f (Link a b) = ([a,b], [(a,b)])
    f (Link2 (a,b) c) = ([a,b,c], [(a,c),(b,c)])
    f (Link3 (a,b,c) d) = ([a,b,c,d], [(a,d),(b,d),(c,d)])
    f _ = error "not implemented"
