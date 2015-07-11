{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE UndecidableInstances #-}
module Scientific.Workflow.Serialization.Show where

import qualified Data.ByteString.Char8             as B

import           Scientific.Workflow.Serialization

instance (Read a, Show a) => Serializable a where
    serialize = B.pack . show
    deserialize = read . B.unpack
