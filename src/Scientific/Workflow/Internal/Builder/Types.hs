{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Scientific.Workflow.Internal.Builder.Types where

import           Control.Lens                      (makeLenses)
import           Control.Monad.State               (State)
import           Data.Graph.Inductive.PatriciaTree (Gr)
import           Data.Serialize                    (Serialize)
import           Data.Serialize.Text               ()
import           Data.Text                         (Text)
import           GHC.Generics                      (Generic)
import           Language.Haskell.TH               (ExpQ, Name, varE)

-- | A computation node.
data Node = Node
    { _nodePid      :: Text
    , _nodeFunction :: ExpQ
    , _nodeAttr     :: Attribute
    }

-- | Links between computational nodes
data Edge = Edge
    { _edgeFrom :: Text
    , _edgeTo   :: Text
    , _edgeOrd  :: EdgeOrd  -- ^ Order of the edge
    }

type EdgeOrd = Int

type Builder = State ([Node], [Edge])

-- | Node attribute
data Attribute = Attribute
    { _label          :: Text      -- ^ Short description
    , _note           :: Text      -- ^ Long description
    , _batch          :: Int         -- ^ Batch size. If > 0, inputs will be divided
                                     -- into batches.
    , _submitToRemote :: Maybe Bool  -- ^ Overwrite the global option
    , _stateful       :: Bool        -- ^ Whether the node function has access
                                     -- to internal states
    , _remoteParam    :: String
    } deriving (Generic)

instance Serialize Attribute

makeLenses 'Attribute

defaultAttribute :: Attribute
defaultAttribute = Attribute
    { _label = ""
    , _note = ""
    , _batch = -1
    , _submitToRemote = Nothing
    , _stateful = False
    , _remoteParam = ""
    }

type AttributeSetter = State Attribute ()

type DAG = Gr Node EdgeOrd

-- | Objects that can be converted to ExpQ
class ToExpQ a where
    toExpQ :: a -> ExpQ

instance ToExpQ Name where
    toExpQ = varE

instance ToExpQ ExpQ where
    toExpQ = id
