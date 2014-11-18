{-# LANGUAGE RecordWildCards,PatternGuards,Rank2Types #-}


module Network.Nats.Json (
    subscribe
  , publish
) where

import Network.Nats (Nats, NatsSID)
import qualified Network.Nats as N
import qualified Data.Aeson as AE

-- | Publish a message
publish :: AE.ToJSON a =>
    Nats 
    -> String -- ^ Subject
    -> a -- ^ Data
    -> IO ()
publish nats subject body = N.publish nats subject (AE.encode body)

-- | Subscribe to a channel, optionally specifying queue group 
-- If the JSON cannot be properly parsed, the message is ignored
subscribe :: AE.FromJSON a =>
    Nats 
    -> String -- ^ Subject
    -> (Maybe String) -- ^ Queue
    -> (NatsSID -- ^ SID of subscription
        -> String -- ^ Subject
        -> a -- ^ Message
        -> Maybe String -- ^ Reply subject
        -> IO ()
        )
    -- ^ Callback
    -> IO NatsSID -- ^ SID of subscription
subscribe nats subject queue jcallback = N.subscribe nats subject queue cb
    where
        cb sid subj msg repl
            | Just body <- AE.decode msg = jcallback sid subj body repl
            | True                       = return () -- Ignore when there is an error decoding
