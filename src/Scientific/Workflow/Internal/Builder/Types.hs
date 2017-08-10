{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Scientific.Workflow.Internal.Builder.Types where

import           Control.Lens                      (makeLenses)
import           Control.Monad.State               (State)
import           Data.Aeson.Types                  (defaultOptions,
                                                    genericParseJSON,
                                                    genericToEncoding)
import           Data.Graph.Inductive.PatriciaTree (Gr)
import           Data.Serialize                    (Serialize)
import           Data.Serialize.Text               ()
import           Data.Text                         (Text)
import           Data.Yaml                         (FromJSON (..), ToJSON (..))
import           GHC.Generics                      (Generic)
import           Instances.TH.Lift                 ()
import           Language.Haskell.TH               (ExpQ, Name, varE)
import           Language.Haskell.TH.Lift          (deriveLift)

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

-- | Node attributes.
data Attribute = Attribute
    { _label          :: Text      -- ^ Short description
    , _note           :: Text      -- ^ Long description
    , _submitToRemote :: Maybe Bool  -- ^ Overwrite the global option
    , _remoteParam    :: String     -- ^ Parameters for to remote execution
    , _functionConfig :: FunctionConfig  -- ^ Usually not being used directly
    } deriving (Generic)

-- | The type of node function
data FunctionConfig = FunctionConfig ParallelMode FunctionType deriving (Generic)

data ParallelMode = None            -- ^ No parallelism.
                  | Standard Int    -- ^ Turn input @a@ into @[a]@ and process
                                    -- them in parallel.
                  | ShareData Int   -- ^ Assume the input is @ContextData d a@,
                                    -- where @d@ is shared and @a@ becomes @[a]@.
                  deriving (Generic)

data FunctionType = Pure       -- ^ The function is pure, i.e., @a -> b@.
                  | IOAction   -- ^ A IO function, i.e., @a -> IO b@.
                  | Stateful   -- ^ A function that has access to configuration,
                               -- i.e., @a -> WorkflowConfig config b@.
                  deriving (Generic)

instance Serialize Attribute
instance Serialize FunctionConfig
instance Serialize ParallelMode
instance Serialize FunctionType

deriveLift ''FunctionConfig
deriveLift ''ParallelMode
deriveLift ''FunctionType
deriveLift ''Attribute

makeLenses 'Attribute

defaultAttribute :: Attribute
defaultAttribute = Attribute
    { _label = ""
    , _note = ""
    , _submitToRemote = Nothing
    , _remoteParam = ""
    , _functionConfig = FunctionConfig None IOAction
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

-- | Data and its environment.
data ContextData context dat = ContextData
    { _context :: context
    , _data    :: dat
    } deriving (Generic)

instance (FromJSON c, FromJSON d) => FromJSON (ContextData c d) where
    parseJSON = genericParseJSON defaultOptions

instance (ToJSON c, ToJSON d) => ToJSON (ContextData c d) where
    toEncoding = genericToEncoding defaultOptions

instance (Serialize c, Serialize d) => Serialize (ContextData c d)
