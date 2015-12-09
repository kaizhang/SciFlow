module Scientific.Workflow.Types where

import qualified Data.HashMap.Strict as M

data WorkflowState = WorkflowState (M.HashMap T.Text Bool)

query :: PID -> WorkflowState -> Bool
query = undefined

type PID = T.Text

data Processor a = Processor
    { pid :: PID
    , runProcessor :: StateT WorkflowState IO a
    }

instance Functor Processor where
    fmap f = fmap . runProcessor

instance Monad Processor where
    return = Processor . return
    m >>= f = runProcessor >>= ( do
        query

mkProc :: PID -> (a -> IO b) -> (a -> Processor (Maybe b))
mkProc p f = \x -> do
    db <- get
    case query p db of
        Finished x -> return $ Just x
        Scheduled -> return $ Just $ f x
        Skip -> return Nothing
