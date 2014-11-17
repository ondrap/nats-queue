nats-queue
==========

Haskell API for NATS messaging system

```
{-# LANGUAGE OverloadedStrings #-}
import qualified Data.ByteString.Lazy as BL
import Network.Nats
import Control.Concurrent

main :: IO ()
main = do
    nats <- connect "nats://user:password@localhost:4222"

    sid <- subscribe nats "news" Nothing $ \_ _ msg _ -> putStrLn $ show msg

    publish nats "news" "I got news for you"
    threadDelay 1000000

    unsubscribe nats sid

    subscribe nats "gift" Nothing $ \_ _ msg mreply -> do
        putStrLn $ show msg
        case mreply of
            Nothing -> return ()
            Just reply -> publish nats reply "I've got a gift for you."

    reply <- request nats "gift" "Do you have anything for me?"

    putStrLn $ show reply
```
