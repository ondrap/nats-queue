{-# LANGUAGE OverloadedStrings #-}

module Network.NatsSpec where

import Network.Nats
import Test.Hspec
import Data.IORef
import Control.Concurrent (threadDelay)
import Data.ByteString.Lazy ()

natsUrl =  "nats://nats:nats@localhost:4222"

-- fakeSettings = defaultSettings {
--         natsHosts=[(NatsHost "127.0.0.1" 5222 "nats" "nats"), 
--                    (NatsHost "127.0.0.1" 5223 "nats" "nats"), 
--                    (NatsHost "127.0.0.1" 5224 "nats" "nats")],
--         natsOnConnect=onconnect,
--         natsOnDisconnect=ondisconnect
--     }

spec :: Spec
spec = describe "Client" $ do
    it ("connects and disconnects to " ++ natsUrl) $ do
        nats <- connect natsUrl
        disconnect nats
    
    it "sends and receives message" $ do
        nats <- connect natsUrl
        mcount <- newIORef 0
        sid <- subscribe nats "nats.test" Nothing $ \_ _ _ _ -> do
            atomicModifyIORef mcount (\x -> (x+1, ()))
        publish nats "nats.test" "Hello"
        threadDelay 100000
        unsubscribe nats sid
        publish nats "nats.test" "Hello2"
        threadDelay 100000
        disconnect nats
        newcount <- readIORef mcount
        newcount `shouldBe` 1


main :: IO ()
main = hspec spec
