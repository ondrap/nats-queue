{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}

module Network.Nats (
    -- * How to use this module
    -- |
    -- @
    --
    -- {-\# LANGUAGE OverloadedStrings \#-}
    --
    -- import qualified Data.ByteString.Lazy as BL
    --
    -- nats <- 'connect' \"nats:\/\/user:password\@localhost:4222\"
    --
    -- sid \<- 'subscribe' nats \"news\" Nothing $ \\_ _ msg _ -\> putStrLn $ show msg
    --
    -- 'publish' nats \"news\" \"I got news for you\"
    --
    -- 'unsubscribe' nats sid
    --
    -- 'subscribe' nats \"gift\" Nothing $ \\_ _ msg mreply -> do
    --     putStrLn $ show msg
    --     case mreply of
    --        Nothing -> return ()
    --        Just reply -> 'publish' nats reply \"I've got a gift for you.\"
    --
    -- reply <- 'request' nats \"gift\" \"Do you have anything for me?\"
    --
    -- putStrLn $ show reply
    -- @
    --
    -- The 'connect' call connects to the NATS server and creates a receiver thread. The
    -- callbacks are run synchronously on this thread when a server messages comes.
    -- Client commands are generally acknowledged by the server with an +OK message,
    -- the library waits for acknowledgment only for the 'subscribe' command. The NATS
    -- server usually closes the connection when there is an error.

    -- * Comparison to API in other languages
    -- |Compared to API in other languages, the Haskell binding does
    -- not implement timeouts and automatic unsubscribing, the 'request' call is implemented
    -- as a synchronous call.
    --
    -- The timeouts can be easily implemented using 'System.Timeout' module, automatic unsubscribing
    -- can be done in the callback function.

    -- * Error behaviour
    -- |The 'connect' function tries to connect to the NATS server. In case of failure it immediately fails.
    -- If there is an error during operations, the NATS module tries to reconnect to the server.
    -- When there are more servers, the client immediately tries to connect to the next server. If
    -- that fails, it waits 1s before trying the next server in the NatsSettings list.
    --
    -- During the reconnection, the calls 'subscribe' and 'request' will block. The calls
    -- 'publish' and 'unsubscribe' silently fail (unsubscribe is handled locally, NATS is a messaging
    -- system without guarantees, 'publish' is not guaranteed to succeed anyway).
    -- After reconnecting to the server, the module automatically resubscribes to previously subscribed channels.
    --
    -- If there is a network failure, the nats commands 'subscribe' and 'request'
    -- may fail on an IOexception or NatsException. The 'subscribe'
    -- command is synchronous, it waits until the server responds with +OK. The commands 'publish'
    -- and 'unsubscribe' are asynchronous, no confirmation from server is required and they
    -- should not raise an exception.
    --
    Nats
    , NatsSID
    , connect
    , connectSettings
    , NatsHost(..)
    , NatsSettings(..)
    , defaultSettings
    -- * Exceptions
    , NatsException
    -- * Access
    , MsgCallback
    , subscribe
    , unsubscribe
    , publish
    , request
    , requestMany
    -- * Termination
    , disconnect
) where


import           Control.Applicative        ((<$>), (<*>))
import           Control.Concurrent
import           Control.Concurrent.Async   (concurrently)
import           Control.Exception          (AsyncException, Exception,
                                             Handler (..), IOException,
                                             SomeException, bracket,
                                             bracketOnError, catch, catches,
                                             throwIO)
import           Control.Monad              (forever, replicateM, unless, void,
                                             when, mzero)
import           Data.Dequeue               as D
import qualified Data.Foldable              as FOLD
import           Data.IORef
import           Data.Maybe                 (fromMaybe)
import           Data.Typeable
import           Network.Socket             (SockAddr (..),
                                             SocketOption (KeepAlive, NoDelay),
                                             getAddrInfo, setSocketOption)
import qualified Network.Socket             as S
import           System.IO
import           System.Random              (randomRIO)
import           System.Timeout

import qualified Data.ByteString.Char8      as BS
import qualified Data.ByteString.Lazy.Char8 as BL
import           Data.Char                  (isUpper, toLower)
import qualified Data.Map.Strict            as Map
import qualified Data.Text                  as T
import           Data.Text.Encoding         (decodeUtf8)

import qualified Data.Aeson                 as AE
import Data.Aeson ((.:), (.!=))
import           Data.Aeson.TH              (defaultOptions, deriveJSON,
                                             fieldLabelModifier)

import qualified Network.URI                as URI

-- | How often should we ping the server
pingInterval :: Int
pingInterval = 3000000

-- | Timeout interval for connect operations
timeoutInterval :: Int
timeoutInterval = 1000000

-- | NATS communication error
data NatsException = NatsException String
    deriving (Show, Typeable)
instance Exception NatsException

data NatsConnectionOptions = NatsConnectionOptions {
        natsConnUser          :: String
        , natsConnPass        :: String
        , natsConnVerbose     :: Bool
        , natsConnPedantic    :: Bool
        , natsConnSslRequired :: Bool
    } deriving (Show)

defaultConnectionOptions :: NatsConnectionOptions
defaultConnectionOptions = NatsConnectionOptions{natsConnUser="nats",natsConnPass="nats", natsConnVerbose=True,
                                                natsConnPedantic=True, natsConnSslRequired=False}

$(deriveJSON defaultOptions{fieldLabelModifier =
            let insertUnderscore acc chr
                    | isUpper chr = chr : '_' : acc
                    | otherwise   = chr : acc
            in
                map toLower . drop 1 . reverse . foldl insertUnderscore [] . drop 8
        } ''NatsConnectionOptions)

-- | Server information sent usually upon opening a connection to NATS server
data NatsServerInfo = NatsServerInfo {
--     natsSvrServerId :: T.Text
--     , natsSvrVersion :: T.Text
--     , natsSvrMaxPayload :: Int
      natsSvrAuthRequired :: Bool
    } deriving (Show)

$(deriveJSON defaultOptions{fieldLabelModifier =
            let insertUnderscore acc chr
                    | isUpper chr = chr : '_' : acc
                    | otherwise   = chr : acc
            in
                map toLower . drop 1 . reverse . foldl insertUnderscore [] . drop 7
        } ''NatsServerInfo)


newtype NatsSID = NatsSID Int deriving (Num, Ord, Eq)
instance Show NatsSID where
    show (NatsSID num) = show num
instance Read NatsSID where
    readsPrec x1 x2 = map (\(a,rest) -> (NatsSID a, rest)) $ readsPrec x1 x2


type MsgCallback = NatsSID -- ^ SID of subscription
        -> String -- ^ Subject
        -> BL.ByteString -- ^ Message
        -> Maybe String -- ^ Reply subject
        -> IO ()

data NatsSubscription = NatsSubscription {
        subSubject  :: Subject
      , subQueue    :: Maybe Subject
      , subCallback :: MsgCallback
      , subSid      :: NatsSID
    }

type FifoQueue = D.BankersDequeue (Maybe T.Text -> IO ())

-- | Control structure representing a connection to NATS server
data Nats = Nats {
          natsSettings :: NatsSettings
        , natsRuntime :: MVar (Handle, -- Network socket
                               FifoQueue, -- FIFO of sent commands waiting for ack
                               Bool, -- False if we are disconnected
                               MVar () -- Empty mvar that gets full in the moment we connect
                               )
        , natsThreadId :: MVar ThreadId
        , natsNextSid :: IORef NatsSID
        , natsSubMap :: IORef (Map.Map NatsSID NatsSubscription)
    }

-- | Host settings; may have different username/password for each host
data NatsHost = NatsHost {
        natsHHost :: String
      , natsHPort :: Int
      , natsHUser :: String      -- ^ Username for authentication
      , natsHPass :: String  -- ^ Password for authentication
    } deriving (Show)
instance AE.FromJSON NatsHost where
  parseJSON (AE.Object v) =
    NatsHost <$>
      v .: "host" <*>
      v .: "port" .!= 4222 <*>
      v .: "user" .!= "nats" <*>
      v .: "pass" .!= "pass"
  parseJSON _ = mzero

-- | Advanced settings for connecting to NATS server
data NatsSettings = NatsSettings {
        natsHosts        :: [NatsHost]
      , natsOnReconnect  :: Nats -> (String, Int) -> IO ()
        -- ^ Called when a client has successfully re-connected. This callback is called synchronously
        --   before the processing of incoming messages begins. It is not called when the client
        --   connects the first time, as such connection is synchronous.
      , natsOnDisconnect :: Nats -> String -> IO ()
        -- ^ Called when a client is disconnected.
    }

defaultSettings :: NatsSettings
defaultSettings = NatsSettings {
        natsHosts = [NatsHost "localhost" 4222 "nats" "nats"]
      , natsOnReconnect = \_ _ -> (return ())
      , natsOnDisconnect = \_ _ -> (return ())
    }

-- | Message received by the client from the server
data NatsSvrMessage =
    NatsSvrMsg { msgSubject::String, msgSid::NatsSID, msgText::BS.ByteString, msgReply::Maybe String}
    | NatsSvrOK
    | NatsSvrError T.Text
    | NatsSvrPing
    | NatsSvrPong
    | NatsSvrInfo NatsServerInfo
    deriving (Show)

newtype Subject = Subject String deriving (Show)

subjectToStr :: Subject -> String
subjectToStr (Subject str) = str

makeSubject :: String -> Subject
makeSubject ""        = error "Empty subject"
makeSubject str
    | any (<=' ') str = error $ "Subject contains incorrect characters: " ++ str
    | otherwise       = Subject str

-- | Message sent from the client to server
data NatsClntMessage =
    NatsClntPing
    | NatsClntPong
    | NatsClntSubscribe Subject NatsSID (Maybe Subject)
    | NatsClntUnsubscribe NatsSID
    | NatsClntPublish Subject (Maybe Subject) BL.ByteString
    | NatsClntConnect NatsConnectionOptions

-- | Encode NATS client message
makeClntMsg :: NatsClntMessage -> BL.ByteString
makeClntMsg = BL.fromChunks . _makeClntMsg
    where
        _makeClntMsg :: NatsClntMessage -> [BS.ByteString]
        _makeClntMsg NatsClntPing = ["PING"]
        _makeClntMsg NatsClntPong = ["PONG"]
        _makeClntMsg (NatsClntSubscribe subject sid (Just queue)) = [BS.pack $ "SUB " ++ subjectToStr subject ++ " " ++ subjectToStr queue ++ " " ++ show sid]
        _makeClntMsg (NatsClntSubscribe subject sid Nothing) = [BS.pack $ "SUB " ++ subjectToStr subject ++ " " ++ show sid]
        _makeClntMsg (NatsClntUnsubscribe sid) = [ BS.pack $ "UNSUB " ++ show sid ]
        _makeClntMsg (NatsClntPublish subj Nothing msg) =
            BS.pack ("PUB " ++ subjectToStr subj ++ " " ++ show (BL.length msg) ++ "\r\n") : BL.toChunks msg
        _makeClntMsg (NatsClntPublish subj (Just reply) msg) =
            BS.pack ("PUB " ++ subjectToStr subj ++ " " ++ subjectToStr reply ++ " " ++ show (BL.length msg) ++ "\r\n") : BL.toChunks msg
        _makeClntMsg (NatsClntConnect info) = "CONNECT " : BL.toChunks (AE.encode info)

-- | Decode NATS server message; result is message + payload (payload is 'undefined' in NatsSvrMsg)
decodeMessage :: BS.ByteString -> Maybe (NatsSvrMessage, Maybe Int)
decodeMessage line = decodeMessage_ mid (BS.drop 1 mrest)
    where
        (mid, mrest) = BS.span (\x -> x/=' ' && x/='\r') line
        decodeMessage_ :: BS.ByteString -> BS.ByteString -> Maybe (NatsSvrMessage, Maybe Int)
        decodeMessage_ "PING" _ = Just (NatsSvrPing, Nothing)
        decodeMessage_ "PONG" _ = Just (NatsSvrPong, Nothing)
        decodeMessage_ "+OK" _ = Just (NatsSvrOK, Nothing)
        decodeMessage_ "-ERR" msg = Just (NatsSvrError (decodeUtf8 msg), Nothing)
        decodeMessage_ "INFO" msg = do
            info <- AE.decode $ BL.fromChunks [msg]
            return (NatsSvrInfo info, Nothing)
        decodeMessage_ "MSG" msg =
            case map BS.unpack (BS.words msg) of
                 [subj, sid, len] -> return (NatsSvrMsg subj (read sid) undefined Nothing, Just $ read len)
                 [subj, sid, reply, len] -> return (NatsSvrMsg subj (read sid) undefined (Just reply), Just $ read len)
                 _ -> fail ""
        decodeMessage_ _ _ = Nothing


-- | Returns next sid and updates MVar
newNatsSid :: Nats -> IO NatsSID
newNatsSid nats = atomicModifyIORef' (natsNextSid nats) $ \sid -> (sid + 1, sid)

-- | Generates a new INBOX name for request/response communication
newInbox :: IO String
newInbox = do
    rnd <- replicateM 13 (randomRIO ('a', 'z'))
    return $ "_INBOX." ++ rnd

-- | Create a TCP connection to the server
connectToServer :: String -> Int -> IO Handle
connectToServer hostname port = do
    addrinfos <- getAddrInfo Nothing (Just hostname) Nothing
    let serveraddr = head addrinfos
    -- Create a socket
    bracketOnError
        (S.socket (S.addrFamily serveraddr) S.Stream S.defaultProtocol)
        S.sClose
        (\sock -> do

            setSocketOption sock KeepAlive 1
            setSocketOption sock NoDelay 1
            let connaddr = case S.addrAddress serveraddr of
                    SockAddrInet _ haddr -> SockAddrInet (fromInteger $ toInteger port) haddr
                    SockAddrInet6 _ finfo haddr scopeid -> SockAddrInet6 (fromInteger $ toInteger port) finfo haddr scopeid
                    other -> other
            S.connect sock connaddr
            h <- S.socketToHandle sock ReadWriteMode
            hSetBuffering h NoBuffering
            return h
        )


ensureConnection :: Nats
    -> Bool -- ^ If true, wait for the connection to become available
    -> ((Handle, FifoQueue) -> IO FifoQueue) -- ^ Action to do when the connection is available
    -> IO ()
-- Block if we are disconnected
ensureConnection nats True f =
    bracketOnError
        (takeMVar $ natsRuntime nats)
        (putMVar $ natsRuntime nats)
        (\r@(handle, _, x1, x2) -> do
            result <- runAction r
            case result of
                 Just nqueue -> putMVar (natsRuntime nats) (handle, nqueue, x1, x2)
                 Nothing -> return ()
        )
    where
        -- Connected
        runAction (handle, queue, True, _) = do
            nqueue <- f (handle, queue)
            return $ Just nqueue
        -- Disconnected, we will wait
        runAction r@(_, _, False, csig) = do
            putMVar (natsRuntime nats) r
            readMVar csig -- Wait for connection to become available
            ensureConnection nats True f
            return Nothing
-- Do not block if we are disconnected
ensureConnection nats False f = modifyMVarMasked_ (natsRuntime nats) runAction
    where
        -- Connected, try to send
        runAction (handle, queue, True, csig) = do
            nqueue <- f (handle, queue)
            return (handle, nqueue, True, csig)
        -- Disconnected, ignore
        runAction (handle, queue, False, csig) =
            return (handle, queue, False, csig)

-- | Send a message and register callback if possible
sendMessage :: Nats -> Bool -> NatsClntMessage -> Maybe (Maybe T.Text -> IO ()) -> IO ()
sendMessage nats blockIfDisconnected msg mcb
    | Just cb <- mcb, supportsCallback msg =
        ensureConnection nats blockIfDisconnected $ \(handle, queue) -> do
            _sendMessage handle msg
            return $ D.pushBack queue cb -- Append callback on the callback queue
    | supportsCallback msg = sendMessage nats blockIfDisconnected msg (Just $ \_ -> return ())
    | Just _ <- mcb, not (supportsCallback msg) = error "Callback not supported"
    | otherwise = ensureConnection nats blockIfDisconnected $ \(handle, queue) -> do
        _sendMessage handle msg
        return queue
    where
        supportsCallback (NatsClntConnect {}) = True
        supportsCallback (NatsClntPublish {}) = True
        supportsCallback (NatsClntSubscribe {}) = True
        supportsCallback (NatsClntUnsubscribe {}) = True
        supportsCallback _ = False

-- Throw exception if an action does not end in specified time
timeoutThrow :: Int -> IO a -> IO a
timeoutThrow t f = do
    res <- timeout t f
    case res of
         Just x -> return x
         Nothing -> throwIO $ NatsException "Reached timeout"

_sendMessage :: Handle -> NatsClntMessage -> IO ()
_sendMessage handle cmsg = timeoutThrow timeoutInterval $ do
    let msg = makeClntMsg cmsg
    case () of
       _| BL.length msg < 1024 ->
                BS.hPut handle $ BS.concat $ BL.toChunks msg ++ ["\r\n"]
        | otherwise -> do
                BL.hPut handle msg
                BS.hPut handle "\r\n"

-- | Do the authentication handshake if necessary
authenticate :: Handle -> String -> String -> IO ()
authenticate handle user password = do
    info <- BS.hGetLine handle
    case decodeMessage info of
        Just (NatsSvrInfo (NatsServerInfo {natsSvrAuthRequired=True}), Nothing) -> do
            let coptions = defaultConnectionOptions{natsConnUser=user, natsConnPass=password}
            BL.hPut handle $ makeClntMsg (NatsClntConnect coptions)
            BS.hPut handle "\r\n"
            response <- BS.hGetLine handle
            case decodeMessage response of
                 Just (NatsSvrOK, Nothing) -> return ()
                 Just (NatsSvrError err, Nothing)-> throwIO $ NatsException $ "Authentication error: " ++ show err
                 _ -> throwIO $ NatsException "Incorrect server response"
        Just (NatsSvrInfo _, Nothing) -> return ()
        _ -> throwIO $ NatsException "Incorrect input from server"

-- | Open and authenticate a connection
prepareConnection :: Nats -> NatsHost -> IO ()
prepareConnection nats nhost = timeoutThrow timeoutInterval $
    bracketOnError
        (connectToServer (natsHHost nhost) (natsHPort nhost))
        hClose
        (\handle -> do
            authenticate handle (natsHUser nhost) (natsHPass nhost)
            csig <- modifyMVar (natsRuntime nats) $ \(_,_,_, csig) ->
                return ((handle, D.empty, True, undefined), csig)
            putMVar csig ()
        )

-- | Main thread that reads events from NATS server and reconnects if necessary
connectionThread :: Bool
                    -> Nats
                    -> [NatsHost] -- ^ inifinite list of connections to try
                    -> IO ()
connectionThread _ _ [] = error "Empty list of connections"
connectionThread firstTime nats (thisconn:nextconn) = do
    mnewconnlist <-
        (connectionHandler firstTime nats thisconn >> return Nothing) -- connectionHandler never returns...
            `catches` [Handler (\(e :: IOException) -> Just <$> errorHandler e),
                       Handler (\(e :: NatsException) -> Just <$> errorHandler e),
                       Handler (\e -> finalHandler e >> return Nothing)]
    case mnewconnlist of
         Nothing -> return () -- Never happens
         Just newconnlist -> connectionThread False nats newconnlist
    where
        finalize :: (Show e) => e -> IO ()
        finalize e = do
            -- Hide existing connection
            (handle, queue, _, _) <- takeMVar (natsRuntime nats)
            finsignal <- newEmptyMVar
            putMVar (natsRuntime nats) (undefined, undefined, False, finsignal)
            -- Close network socket
            hClose handle
            -- Call appropriate callbacks on unfinished calls
            FOLD.mapM_ (\f -> f $ Just (T.pack $ show e)) queue
            -- Call user supplied disconnect
            (natsOnDisconnect $ natsSettings nats) nats (show e)

        errorHandler :: (Show e) => e -> IO [NatsHost]
        errorHandler e = do
            finalize e
            tryToConnect nextconn
            where
                tryToConnect connlist@(conn:rest) = do
                    res <- (prepareConnection nats conn >> return (Just connlist))
                        `catches` [ Handler (\(_ :: IOException) -> return Nothing),
                                    Handler (\(_ :: NatsException) -> return Nothing) ]
                    case res of
                         Just restlist -> return restlist
                         Nothing       -> threadDelay timeoutInterval >> tryToConnect rest
                tryToConnect [] = error "Empty list of connections"

        -- Handler for exiting the thread
        finalHandler :: AsyncException -> IO ()
        finalHandler = finalize

pingerThread :: Nats -> IORef (Int, Int) -> IO ()
pingerThread nats pingStatus = forever $ do
    threadDelay pingInterval
    -- todo - kontrola pingu
    ok <- atomicModifyIORef' pingStatus $ \(pings, pongs) -> ((pings+1, pongs), pings - pongs < 2)
    unless  ok $ throwIO (NatsException "Ping timeouted")
    sendMessage nats True NatsClntPing Nothing

-- | Forever read input from a connection and process it
connectionHandler :: Bool -> Nats -> NatsHost -> IO ()
connectionHandler firstTime nats (NatsHost host port _ _) = do
    (handle, _, _, _) <- readMVar (natsRuntime nats)
    -- Subscribe channels that are supposed to be subscribed
    subscriptions <- readIORef (natsSubMap nats)
    FOLD.forM_ subscriptions $ \(NatsSubscription subject queue _ sid) ->
        sendMessage nats True (NatsClntSubscribe subject sid queue) Nothing

    -- Call user function that we are successfully connected
    unless firstTime $ (natsOnReconnect $ natsSettings nats) nats (host, port)
    -- Allocate structures for PING, IORef is probably easiest to manage
    pingStatus <- newIORef (0, 0)

    -- Perform the job
    void $ concurrently
            (pingerThread nats pingStatus)
            (connectionHandler' handle nats pingStatus)

connectionHandler' :: Handle -> Nats -> IORef (Int, Int) -> IO ()
connectionHandler' handle nats pingStatus = forever $ do
    line <- BS.hGetLine handle
    case decodeMessage line of
        Just (msg, Nothing) ->
            handleMessage msg
        Just (msg@(NatsSvrMsg {}), Just paylen) -> do
            payload <- BS.hGet handle (paylen + 2) -- +2 = CRLF
            handleMessage msg{msgText=BS.take paylen payload}
        _ ->
            putStrLn $ "Incorrect message: " ++ show line

    where
        -- | Pull callback for OK/ERR status from FIFO queue
        popCb (h, queue, x1, x2) = return ((h, newq, x1, x2), item)
            where
                (item, newq) = case D.popFront queue of
                    Just inq -> inq
                    Nothing  -> (maybe (return ()) print, D.empty)
        handleMessage NatsSvrPing = sendMessage nats True NatsClntPong Nothing
        handleMessage NatsSvrPong =
            atomicModifyIORef' pingStatus $
                \(pings, pongs) -> ((pings, pongs + 1), ())

        handleMessage NatsSvrOK = do
            cb <- modifyMVar (natsRuntime nats) popCb
            cb Nothing
        handleMessage (NatsSvrError txt) = do
            cb <- modifyMVar (natsRuntime nats) popCb
            cb $ Just txt
        handleMessage (NatsSvrInfo _) = return ()
        handleMessage (NatsSvrMsg {..}) = do
            msubscription <- Map.lookup msgSid <$> readIORef (natsSubMap nats)
            case msubscription of
                Just subscription ->
                    subCallback subscription msgSid msgSubject (BL.fromChunks [msgText]) msgReply
                    `catch`
                        (\(e :: SomeException) -> print e)
                -- SID not found in map, force unsubscribe
                Nothing -> sendMessage nats True (NatsClntUnsubscribe msgSid) Nothing

-- | Connect to a NATS server
connect :: String -- ^ URI with format: nats:\/\/user:password\@localhost:4222
    -> IO Nats
connect uri = do
    let parsedUri = fromMaybe (error ("Error parsing NATS url: " ++ uri))
                      (URI.parseURI uri)
    when (URI.uriScheme parsedUri /= "nats:") $ error "Incorrect URL scheme"

    let (host, port, user, password) = case URI.uriAuthority parsedUri of
                Just (URI.URIAuth {..}) -> (uriRegName,
                                          read $ drop 1 uriPort,
                                          takeWhile (/= ':') uriUserInfo,
                                          takeWhile (/= '@') $ drop 1 $ dropWhile (/= ':') uriUserInfo
                                          )
                Nothing -> error "Missing hostname section"
    connectSettings defaultSettings{
            natsHosts=[NatsHost host port user password]
        }

-- | Connect to NATS server using custom settings
connectSettings :: NatsSettings -> IO Nats
connectSettings settings = do
    csig <- newEmptyMVar
    mruntime <- newMVar (undefined, undefined, False, csig)
    mthreadid <- newEmptyMVar
    nextsid <- newIORef 1
    submap <- newIORef Map.empty
    let nats = Nats{
              natsSettings=settings
            , natsRuntime=mruntime
            , natsThreadId=mthreadid
            , natsNextSid=nextsid
            , natsSubMap=submap
        }
        hosts = natsHosts settings

    -- Try to connect to all until one succeeds
    connhost <- tryUntilSuccess hosts $ prepareConnection nats
    threadid <- forkIO $ connectionThread True nats (connhost : cycle hosts)
    putMVar mthreadid threadid
    return nats
    where
        tryUntilSuccess [a] f = f a >> return a
        tryUntilSuccess (a:rest) f = (f a >> return a) `catch` (\(_ :: SomeException) -> tryUntilSuccess rest f)
        tryUntilSuccess [] _ = error "Empty list"

-- | Subscribe to a channel, optionally specifying queue group
subscribe :: Nats
    -> String -- ^ Subject
    -> Maybe String -- ^ Queue
    -> MsgCallback -- ^ Callback
    -> IO NatsSID -- ^ SID of subscription
subscribe nats subject queue cb =
    let
        ssubject = makeSubject subject
        squeue = makeSubject `fmap` queue
        addToSubTable sid = atomicModifyIORef' (natsSubMap nats) $ \submap ->
                (Map.insert sid NatsSubscription{subSubject=ssubject, subQueue=squeue, subCallback=cb, subSid=sid} submap, ())
    in do
        mvar <- newEmptyMVar :: IO (MVar (Maybe T.Text))
        sid <- newNatsSid nats
        sendMessage nats True (NatsClntSubscribe ssubject sid squeue) $ Just $ \err -> do
            case err of
                Nothing -> addToSubTable sid
                Just _ -> return ()
            putMVar mvar err

        merr <- takeMVar mvar
        case merr of
            Just err -> throwIO $ NatsException $ T.unpack err
            Nothing -> return sid

-- | Unsubscribe from a channel
unsubscribe :: Nats
    -> NatsSID
    -> IO ()
unsubscribe nats sid = do
    -- Remove from internal tables
    atomicModifyIORef' (natsSubMap nats) $ \ioref -> (Map.delete sid ioref, ())
    -- Unsubscribe from server, ignore errors
    sendMessage nats False (NatsClntUnsubscribe sid) Nothing
        `catches` [ Handler (\(_ :: IOException) -> return ()),
                    Handler (\(_ :: NatsException) -> return ()) ]


-- | Synchronous request/response communication to obtain one message
request :: Nats
    -> String             -- ^ Subject
    -> BL.ByteString      -- ^ Request
    -> IO BL.ByteString   -- ^ Response
request nats subject body = do
    mvar <- newEmptyMVar :: IO (MVar (Either String BL.ByteString))
    inbox <- newInbox
    bracket
            (subscribe nats inbox Nothing $ \_ _ response _ ->
                void $ tryPutMVar mvar (Right response)
            )
            (unsubscribe nats)
            (\_ -> do
                sendMessage nats True (NatsClntPublish (makeSubject subject) (Just $ makeSubject inbox) body) $ Just $ \merr ->
                    case merr of
                         Nothing -> return ()
                         Just err -> void $ tryPutMVar mvar (Left $ T.unpack err)
                result <- takeMVar mvar
                case result of
                     Left err -> throwIO $ NatsException err
                     Right res -> return res
            )

-- | Synchronous request/response for obtaining many messages in certain timespan
requestMany :: Nats
    -> String              -- ^ Subject
    -> BL.ByteString       -- ^ Body
    -> Int                 -- ^ Timeout in microseconds
    -> IO [BL.ByteString]
requestMany nats subject body time = do
    result <- newIORef []
    inbox <- newInbox
    bracket
        (subscribe nats inbox Nothing $ \_ _ response _ ->
                atomicModifyIORef result $ \old -> (response:old, ())
        )
        (unsubscribe nats)
        (\_ -> do
            publish' nats subject (Just inbox) body
            threadDelay time
        )
    reverse <$> readIORef result

-- | Publish a message
publish :: Nats
    -> String -- ^ Subject
    -> BL.ByteString -- ^ Data
    -> IO ()
publish nats subject = publish' nats subject Nothing

publish' :: Nats
    -> String -- ^ Subject
    -> Maybe String
    -> BL.ByteString -- ^ Data
    -> IO ()
publish' nats subject inbox body =
    -- Ignore errors - messages can get lost
    sendMessage nats False (NatsClntPublish (makeSubject subject) (makeSubject <$> inbox) body) Nothing
        `catches` [ Handler (\(_ :: IOException) -> return ()),
                    Handler (\(_ :: NatsException) -> return ()) ]

-- | Disconnect from a NATS server
disconnect :: Nats -> IO ()
disconnect nats = do
    threadid <- readMVar (natsThreadId nats)
    killThread threadid
