module Scientific.Workflow.Serialization where

import qualified Data.ByteString as B

class Serializable a where
    serialize :: a -> B.ByteString
    deserialize :: B.ByteString -> a
