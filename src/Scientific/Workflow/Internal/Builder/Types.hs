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
import           Data.Yaml                         (FromJSON (..), ToJSON (..))
import           Data.Aeson.Types (genericParseJSON, genericToEncoding, defaultOptions)
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
    , _submitToRemote :: Maybe Bool  -- ^ Overwrite the global option
    , _remoteParam    :: String
    } deriving (Generic)

instance Serialize Attribute

makeLenses 'Attribute

defaultAttribute :: Attribute
defaultAttribute = Attribute
    { _label = ""
    , _note = ""
    , _submitToRemote = Nothing
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

-- | Data
data ContextData context dat = ContextData
    { _context :: context
    , _data    :: dat
    } deriving (Generic)

instance (FromJSON c, FromJSON d) => FromJSON (ContextData c d) where
    parseJSON = genericParseJSON defaultOptions

instance (ToJSON c, ToJSON d) => ToJSON (ContextData c d) where
    toEncoding = genericToEncoding defaultOptions

instance (Serialize c, Serialize d) => Serialize (ContextData c d)
