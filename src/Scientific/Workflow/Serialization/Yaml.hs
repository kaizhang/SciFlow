{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
module Scientific.Workflow.Serialization.Yaml
    (Serializable(..)
    ) where

import Data.Yaml (FromJSON, ToJSON, encode, decode)
import Data.Maybe (fromJust)

import Scientific.Workflow.Serialization

instance (FromJSON a, ToJSON a) => Serializable a where
    serialize = encode
    deserialize = fromJust . decode
