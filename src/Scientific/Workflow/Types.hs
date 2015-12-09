{-# LANGUAGE FlexibleInstances #-}

module Scientific.Workflow.Types where

import           Control.Monad.State
import qualified Data.HashMap.Strict as M
import qualified Data.Text           as T
import           Language.Haskell.TH
import Control.Lens

type PID = T.Text

data ProcState r = Finished r
                 | Scheduled
                 | Skip

data WorkflowState = WorkflowState (M.HashMap PID Bool)

query :: PID -> WorkflowState -> ProcState r
query = undefined

update :: PID -> r -> WorkflowState -> IO ()
update = undefined

type Processor a = StateT WorkflowState IO a

mkProc :: PID -> (a -> IO b) -> (Maybe a -> Processor (Maybe b))
mkProc p f = \input -> do
    db <- get
    case query p db of
        Finished result -> return $ Just result
        Scheduled -> case input of
            Just x -> lift $ do
                result <- f x
                update p result db
                return $ Just result
            _ -> error "The impossible occurs"
        Skip -> return Nothing

data Attribute = Attribute
    { _label :: T.Text
    }

-- | Factors are small subgraphs/units of workflows. Each factor is associated
-- with multiple inputs and a single output
data Factor = S PID
            | L PID PID
            | L2 (PID, PID) PID
            | L3 (PID, PID, PID) PID
            | L4 (PID, PID, PID, PID) PID
            | L5 (PID, PID, PID, PID, PID) PID
            | L6 (PID, PID, PID, PID, PID, PID) PID

type Builder = State ([(PID, ExpQ, Attribute)], [Factor])

-- | Declare a computational node
node :: PID -> ExpQ -> Attribute -> Builder ()
node p fn attr = modify $ _1 %~ (newNode:)
  where
    newNode = (p, fn, attr)

-- | many-to-one generalized link function
link :: [PID] -> PID -> Builder ()
link [] t = singleton t
link [a] t = link1 a t
link [a,b] t = link2 (a,b) t
link [a,b,c] t = link3 (a,b,c) t
link [a,b,c,d] t = link4 (a,b,c,d) t
link [a,b,c,d,e] t = link5 (a,b,c,d,e) t
link [a,b,c,d,e,f] t = link6 (a,b,c,d,e,f) t
link _ _ = error "I can't have so many links, yet!"

-- | (~>) = link.
(~>) :: [PID] -> PID -> Builder ()
(~>) = link

-- | singleton
singleton :: PID -> Builder ()
singleton t = modify $ _2 %~ (S t :)

-- | Declare a path.
path :: [PID] -> Builder ()
path ns = foldM_ f (head ns) $ tail ns
  where
    f a t = link1 a t >> return t

-- | one-to-one link
link1 :: PID -> PID -> Builder ()
link1 a t = modify $ _2 %~ (L a t :)

-- | two-to-one link
link2 :: (PID, PID) -> PID -> Builder ()
link2 (a,b) t = modify $ _2 %~ (L2 (a, b) t :)

-- | tree-to-one link
link3 :: (PID, PID, PID) -> PID -> Builder ()
link3 (a,b,c) t = modify $ _2 %~ (L3 (a,b,c) t :)

link4 :: (PID, PID, PID, PID) -> PID -> Builder ()
link4 (a,b,c,d) t = modify $ _2 %~ (L4 (a,b,c,d) t :)

link5 :: (PID, PID, PID, PID, PID) -> PID -> Builder ()
link5 (a,b,c,d,e) t = modify $ _2 %~ (L5 (a,b,c,d,e) t :)

link6 :: (PID, PID, PID, PID, PID, PID) -> PID -> Builder ()
link6 (a,b,c,d,e,f) t = modify $ _2 %~ (L6 (a,b,c,d,e,f) t :)

-- | Objects that can be converted to ExpQ
class ToExpQ a where
    toExpQ :: a -> ExpQ

instance ToExpQ Name where
    toExpQ = varE

instance ToExpQ ExpQ where
    toExpQ = id
