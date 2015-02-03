NATS - Haskell client
==========

Haskell API for NATS messaging system (see https://github.com/derekcollison/nats).

* Network failures/reconnections are handled automatically in the background,
  however `subscribe` and `request` functions can still return a network exception.
* It should correctly combine with `System.Timeout`, therefore there is no API
  for timeouts as in other language API.
* There is no automatic unsubscribe after certain number of messages as this
  is easily handled by other means.
* The functions `unsubscribe` and `publish` will not fail. They may take up to
  1 second if there is a problem with network connection. They will not block and not 
  throw an exception if the client is disconnected from the server. 
  (NATS does not guarantee delivery anyway)
* Clustered NATS is supported. NATS servers can be specified by overriding defaultSettings.
  Upon failure, the background thread automatically reconnects to the next available 
  server. Rejected connections and unresponsive servers are handled by closing
  current connection and trying next server (the timeout is 1 second).

Example use:

```haskell
{-# LANGUAGE OverloadedStrings #-}
import qualified Data.ByteString.Lazy as BL
import Network.Nats
import Control.Concurrent

main :: IO ()
main = do
    -- Connect to the NATS server
    nats <- connect "nats://user:password@localhost:4222"
    -- Subscribe to channel "news", we do not use any queue group
    sid <- subscribe nats "news" Nothing $ 
            \_ _ msg _ ->    -- The parameters are (sid, subject, message, reply_subject)
                putStrLn $ show msg
    -- Publish a message (the message parameter is a lazy ByteString)
    publish nats "news" "I got news for you"
    -- Wait so that we can receive the message from the server
    threadDelay 1000000
    -- Unsubscribe from the channel
    unsubscribe nats sid
    -- Subscribe to a "gift" channel
    subscribe nats "gift" Nothing $ \_ _ msg mreply -> do
        putStrLn $ show msg
        case mreply of -- If the sender used a 'request' call, use this subject to send message back
            Nothing -> return ()
            Just reply -> publish nats reply "I've got a gift for you."
    -- Request/response communication with a "gift" channel
    reply <- request nats "gift" "Do you have anything for me?"
    putStrLn $ show reply
```
