{-# LANGUAGE TemplateHaskell,OverloadedStrings,RecordWildCards,GeneralizedNewtypeDeriving,PatternGuards,DeriveDataTypeable, ScopedTypeVariables #-}

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
    -- * Termination
    , disconnect
) where


import System.IO
import Control.Concurrent.MVar
import Control.Concurrent
import qualified Network.Socket as S
import Network.Socket (SocketOption(KeepAlive), setSocketOption, getAddrInfo, SockAddr(..))
import Control.Monad (forever, replicateM)
import Data.Dequeue as D
import Control.Applicative ((<$>))
import Data.Typeable
import qualified Data.Foldable as FOLD
import Control.Exception (bracket, bracketOnError, throwIO, catch, IOException, AsyncException, Exception, SomeException,
                          catches, Handler(..))
import System.Random (randomRIO)
import Data.IORef
import System.Timeout
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Class (lift)
import Data.Time.Clock (getCurrentTime, diffUTCTime, UTCTime)

import qualified Data.Map.Strict as Map
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.ByteString.Char8 as BS
import Data.Char (toLower, isUpper)
import qualified Data.Text as T
import Data.Text.Encoding (decodeUtf8)

import qualified Data.Aeson as AE
import Data.Aeson.TH (deriveJSON, defaultOptions, fieldLabelModifier)

import qualified Network.URI as URI

-- | How often should we ping the server
pingInterval :: Int
pingInterval = 1000000

-- | Timeout interval for connect operations
timeoutInterval :: Int
timeoutInterval = 1000000

-- | NATS communication error
data NatsException = NatsException String
    deriving (Show, Typeable)
instance Exception NatsException

data NatsConnectionOptions = NatsConnectionOptions {
        natsConnUser :: String
        , natsConnPass :: String
        , natsConnVerbose :: Bool
        , natsConnPedantic :: Bool
        , natsConnSslRequired :: Bool
    } deriving (Show)

defaultConnectionOptions :: NatsConnectionOptions
defaultConnectionOptions = NatsConnectionOptions{natsConnUser="nats",natsConnPass="nats", natsConnVerbose=True,
                                                natsConnPedantic=True, natsConnSslRequired=False}
    
$(deriveJSON defaultOptions{fieldLabelModifier =(
            let insertUnderscore acc chr
                    | isUpper chr = chr : '_' : acc
                    | otherwise   = chr : acc
            in 
                map toLower . drop 1 . reverse . foldl insertUnderscore [] . drop 8
        )} ''NatsConnectionOptions)
    
-- | Server information sent usually upon opening a connection to NATS server
data NatsServerInfo = NatsServerInfo {
    natsSvrServerId :: T.Text
    , natsSvrVersion :: T.Text
    , natsSvrMaxPayload :: Int
    , natsSvrAuthRequired :: Bool
    } deriving (Show)
    
$(deriveJSON defaultOptions{fieldLabelModifier =(
            let insertUnderscore acc chr
                    | isUpper chr = chr : '_' : acc
                    | otherwise   = chr : acc
            in 
                map toLower . drop 1 . reverse . foldl insertUnderscore [] . drop 7
        )} ''NatsServerInfo)


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
        subSubject :: Subject
      , subQueue :: Maybe Subject
      , subCallback :: MsgCallback
      , subSid :: NatsSID
    }
    
type FifoQueue = D.BankersDequeue (Maybe T.Text -> IO ())
    
-- | Control structure representing a connection to NATS server
data Nats = Nats {
          natsConnOptions :: NatsConnectionOptions
        , natsSettings :: NatsSettings
        , natsRuntime :: MVar (Handle, -- Network socket
                               FifoQueue, -- FIFO of sent commands waiting for ack
                               Bool, -- False if we are disconnected
                               MVar () -- Empty mvar that gets full in the moment we connect
                               )
        , natsThreadId :: MVar ThreadId
        , natsNextSid :: IORef NatsSID
        , natsSubMap :: IORef (Map.Map NatsSID NatsSubscription)
    }

-- | Advanced settings for connecting to NATS server
data NatsSettings = NatsSettings {
        natsHosts :: [(String, Int)]  -- ^ List of (host,port) NATS servers in a cluster
      , natsUser :: String            -- ^ Username for authentication
      , natsPassword :: String        -- ^ Password for authentication
      , natsOnConnect :: Nats -> (String, Int) -> IO ()  -- ^ Called when a client has successfully connected
      , natsOnDisconnect :: Nats -> String -> IO ()      -- ^ Called when a client was disconnected, parameter is Reason
    }

defaultSettings :: NatsSettings
defaultSettings = (NatsSettings {
        natsHosts = [("localhost", 4222)]
      , natsUser = "nats"
      , natsPassword = "nats"
      , natsOnConnect = \_ _ -> (return ())
      , natsOnDisconnect = \_ _ -> (return ())
    })
    
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
        _makeClntMsg (NatsClntSubscribe subject sid (Just queue)) = [BS.pack $ "SUB " ++ (subjectToStr subject) ++ " " ++ (subjectToStr queue) ++ " " ++ (show sid)]
        _makeClntMsg (NatsClntSubscribe subject sid Nothing) = [BS.pack $ "SUB " ++ (subjectToStr subject) ++ " " ++ (show sid)]
        _makeClntMsg (NatsClntUnsubscribe sid) = [ BS.pack $ "UNSUB " ++ (show sid) ]
        _makeClntMsg (NatsClntPublish subj Nothing msg) = 
            (BS.pack $ "PUB " ++ (subjectToStr subj) ++ " " ++ (show $ BL.length msg) ++ "\r\n") : BL.toChunks msg
        _makeClntMsg (NatsClntPublish subj (Just reply) msg) = 
            (BS.pack $ "PUB " ++ (subjectToStr subj) ++ " " ++ (subjectToStr reply) ++ " " ++ (show $ BL.length msg) ++ "\r\n") : BL.toChunks msg
        _makeClntMsg (NatsClntConnect info) = "CONNECT " : (BL.toChunks $ AE.encode info)
    
-- | Decode NATS server message; result is message + payload (payload is 'undefined' in NatsSvrMsg)
decodeMessage :: BS.ByteString -> Maybe (NatsSvrMessage, Maybe Int)
decodeMessage line = decodeMessage_ mid mpayload
    where 
        (mid, mpayload) = (BS.takeWhile (\x -> x/=' ' && x/='\r') line, 
                             BS.drop 1 $ BS.dropWhile (\x -> x/=' ' && x/='\r') line)

        decodeMessage_ :: BS.ByteString -> BS.ByteString -> Maybe (NatsSvrMessage, Maybe Int)
        decodeMessage_ "PING" _ = Just (NatsSvrPing, Nothing)
        decodeMessage_ "PONG" _ = Just (NatsSvrPong, Nothing)
        decodeMessage_ "+OK" _ = Just (NatsSvrOK, Nothing)
        decodeMessage_ "-ERR" msg = Just (NatsSvrError (decodeUtf8 msg), Nothing)
        decodeMessage_ "INFO" msg = do
            info <- AE.decode $ BL.fromChunks [msg]
            return $ (NatsSvrInfo info, Nothing)
        decodeMessage_ "MSG" msg = do
            let fields = BS.split ' ' msg
            case (map BS.unpack fields) of
                 [subj, sid, len] -> return (NatsSvrMsg subj (read sid) undefined Nothing, Just $ read len)
                 [subj, sid, reply, len] -> return (NatsSvrMsg subj (read sid) undefined (Just $ reply), Just $ read len)
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
    let serveraddr = (head addrinfos)
    -- Create a socket
    bracketOnError
        (S.socket (S.addrFamily serveraddr) S.Stream S.defaultProtocol)
        (S.sClose)
        (\sock -> do
            
            setSocketOption sock KeepAlive 1
            let connaddr = case (S.addrAddress serveraddr) of
                    SockAddrInet _ haddr -> SockAddrInet (fromInteger $ toInteger port) haddr
                    SockAddrInet6 _ finfo haddr scopeid -> SockAddrInet6 (fromInteger $ toInteger port) finfo haddr scopeid
                    other -> other
            S.connect sock connaddr
            h <- S.socketToHandle sock ReadWriteMode
            hSetBuffering h NoBuffering
            return h
        )
        
        
ensureConnection :: Nats -> Bool -> ((Handle, FifoQueue) -> IO FifoQueue) -> IO ()
-- Block if we are disconnected
ensureConnection nats True f = do
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
    | True = ensureConnection nats blockIfDisconnected $ \(handle, queue) -> do
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
    -- TODO: We should change this to something using writev (Socket.sendMany)
    BL.hPut handle $ makeClntMsg cmsg
    BS.hPut handle "\r\n"

-- | Do the authentication handshake if necessary
authenticate :: Nats -> Handle -> IO ()
authenticate nats handle = do
    info <- BS.hGetLine handle
    case (decodeMessage info) of
        Just (NatsSvrInfo (NatsServerInfo {natsSvrAuthRequired=True}), Nothing) -> do
            BL.hPut handle $ makeClntMsg (NatsClntConnect $ natsConnOptions nats)
            BS.hPut handle "\r\n"
            response <- BS.hGetLine handle
            case (decodeMessage response) of
                 Just (NatsSvrOK, Nothing) -> return ()
                 Just (NatsSvrError err, Nothing)-> throwIO $ NatsException $ "Authentication error: " ++ (show err)
                 _ -> throwIO $ NatsException $ "Incorrect server response"
        Just (NatsSvrInfo _, Nothing) -> return ()
        _ -> throwIO $ NatsException "Incorrect input from server"
            
-- | Open and authenticate a connection
prepareConnection :: Nats -> (String, Int) -> IO ()
prepareConnection nats (host, port) = timeoutThrow timeoutInterval $
    bracketOnError
        (connectToServer host port)
        (hClose)
        (\handle -> do
            authenticate nats handle
            csig <- modifyMVar (natsRuntime nats) $ \(_,_,_, csig) ->
                return $ ((handle, D.empty, True, undefined), csig)
            putMVar csig ()
        )

-- | Main thread that reads events from NATS server and reconnects if necessary
connectionThread :: Nats
                    -> [(String, Int)] -- ^ inifinite list of connections to try
                    -> IO ()
connectionThread _ [] = error "Empty list of connections"
connectionThread nats (thisconn:nextconn) = do
    mnewconnlist <- 
        (connectionHandler nats thisconn >> return Nothing) -- connectionHandler never returns...
            `catches` [Handler (\(e :: IOException) -> Just <$> errorHandler e),
                       Handler (\(e :: NatsException) -> Just <$> errorHandler e),
                       Handler (\e -> finalHandler e >> return Nothing)]
    case mnewconnlist of
         Nothing -> return ()
         Just newconnlist -> connectionThread nats newconnlist
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
            
        errorHandler :: (Show e) => e -> IO [(String, Int)]
        errorHandler e = do
            finalize e
            tryToConnect nextconn
            where
                tryToConnect connlist@(conn:rest) = do
                    res <- ((prepareConnection nats conn) >> (return $ Just connlist))
                        `catches` [ Handler (\(_ :: IOException) -> return Nothing),
                                    Handler (\(_ :: NatsException) -> return Nothing) ]
                    case res of
                         Just restlist -> return restlist
                         Nothing       -> threadDelay timeoutInterval >> tryToConnect rest
                tryToConnect [] = error "Empty list of connections"
                        
        -- Handler for exiting the thread
        finalHandler :: AsyncException -> IO ()
        finalHandler e = do
            finalize e

-- | Forever read input from a connection and process it
connectionHandler :: Nats -> (String, Int) -> IO ()
connectionHandler nats (host, port) = do
    (handle, _, _, _) <- readMVar (natsRuntime nats)
    -- Subscribe channels that are supposed to be subscribed
    subscriptions <- readIORef (natsSubMap nats)
    FOLD.forM_ subscriptions $ \(NatsSubscription subject queue _ sid) ->
        sendMessage nats True (NatsClntSubscribe subject sid queue) Nothing
        
    -- Call user function that we are successfully connected
    (natsOnConnect $ natsSettings nats) nats (host, port)
    -- Allocate structures for PING, IORef is probably easiest to manage
    pingStatus <- getCurrentTime >>= \now -> newIORef ((now, 0, 0) :: (UTCTime, Int, Int))
    
    -- Perform the job
    forever $ 
        let
            -- | Pull callback for OK/ERR status from FIFO queue
            popCb (h, queue, x1, x2) = return ((h, newq, x1, x2), item)
                where
                    (item, newq) = D.popFront queue
            handleMessage NatsSvrPing = sendMessage nats True NatsClntPong Nothing
            handleMessage NatsSvrPong = 
                atomicModifyIORef' pingStatus $ 
                    \(time, pings, pongs) -> ((time, pings, pongs + 1), ())
                
            handleMessage NatsSvrOK = do
                cb <- modifyMVar (natsRuntime nats) $ popCb
                case cb of
                     Just f -> f Nothing
                     Nothing -> return () -- This should not happen, spurious OK
            handleMessage (NatsSvrError txt) = do
                cb <- modifyMVar (natsRuntime nats) $ popCb
                case cb of
                     Just f -> f $ Just txt
                     Nothing -> putStrLn $ show txt
            handleMessage (NatsSvrInfo (NatsServerInfo {natsSvrAuthRequired=True})) = do
                sendMessage nats True (NatsClntConnect $ natsConnOptions nats) Nothing
            handleMessage (NatsSvrInfo _) = return ()
            handleMessage (NatsSvrMsg {..}) = do
                msubscription <- Map.lookup msgSid <$> readIORef (natsSubMap nats)
                case msubscription of
                     Just subscription -> 
                        (subCallback subscription) msgSid msgSubject (BL.fromChunks [msgText]) msgReply
                        `catch`
                            (\(e :: SomeException) -> putStrLn $ (show e))
                     -- SID not found in map, force unsubscribe
                     Nothing -> sendMessage nats True (NatsClntUnsubscribe msgSid) Nothing 
                     
            -- Sends ping if needed, aborts if we did not get PONG in time
            checkPingTask = do
                now <- getCurrentTime
                (lastPing, pings, pongs) <- readIORef pingStatus
                if (now `diffUTCTime` lastPing) > 1
                    then do
                        if pings /= pongs 
                           then throwIO (NatsException "Ping timeouted") 
                           else return ()
                        sendMessage nats True NatsClntPing Nothing
                        writeIORef pingStatus (now, pings +1, pongs)
                    else return ()
        in do
            -- Ping code must be synchronously here because of exception handling
            _ <- runMaybeT $ do
                line <- MaybeT $ timeout pingInterval $ BS.hGetLine handle
                decoded <- MaybeT $ return $ decodeMessage line
                lift $ case decoded of
                    (msg, Nothing) -> 
                        handleMessage msg
                    (msg@(NatsSvrMsg {}), Just paylen) -> do
                        payload <- BS.hGet handle (paylen + 2) -- +2 = CRLF
                        handleMessage msg{msgText=BS.take paylen payload}
                    _ -> 
                        putStrLn $ "Incorrect message: " ++ (show line)
            checkPingTask
                        
-- | Connect to a NATS server    
connect :: String -- ^ URI with format: nats:\/\/user:password\@localhost:4222
    -> IO Nats
connect uri = do
    let parsedUri = case (URI.parseURI uri) of 
            Just x -> x
            Nothing -> error ("Error parsing NATS url: " ++ uri)
    if URI.uriScheme parsedUri /= "nats:"
        then error "Incorrect URL scheme"
        else return ()
    
    let (host, port, user, password) = case (URI.uriAuthority parsedUri) of
                Just (URI.URIAuth {..}) -> (uriRegName, 
                                          read $ drop 1 uriPort,
                                          takeWhile (\x -> x /= ':') uriUserInfo,
                                          takeWhile (\x -> x /= '@') $ drop 1 $ dropWhile (\x -> x /= ':') uriUserInfo
                                          )
                Nothing -> error "Missing hostname section"
    connectSettings defaultSettings{
            natsHosts=[(host, port)],
            natsUser=user,
            natsPassword=password
        }

-- | Connect to NATS server using custom settings
connectSettings :: NatsSettings -> IO Nats                
connectSettings settings = do
    csig <- newEmptyMVar
    mruntime <- newMVar (undefined, undefined, False, csig)
    mthreadid <- newEmptyMVar 
    nextsid <- newIORef 1
    submap <- newIORef Map.empty
    let opts = defaultConnectionOptions{natsConnUser=(natsUser settings), natsConnPass=(natsPassword settings)} 
        nats = Nats{
            natsConnOptions=opts 
            , natsSettings=settings
            , natsRuntime=mruntime 
            , natsThreadId=mthreadid 
            , natsNextSid=nextsid 
            , natsSubMap=submap
        }
        hosts = (natsHosts settings)
    
    -- Try to connect to all until one succeeds
    connhost <- tryUntilSuccess hosts $ prepareConnection nats
    threadid <- forkIO $ connectionThread nats (connhost:(cycle hosts))
    putMVar mthreadid threadid
    return nats
    where
        tryUntilSuccess [a] f = f a >> return a
        tryUntilSuccess (a:rest) f = (f a >> return a) `catch` (\(_ :: SomeException) -> tryUntilSuccess rest f)
        tryUntilSuccess [] _ = error "Empty list"
    
-- | Subscribe to a channel, optionally specifying queue group 
subscribe :: Nats 
    -> String -- ^ Subject
    -> (Maybe String) -- ^ Queue
    -> MsgCallback -- ^ Callback
    -> IO NatsSID -- ^ SID of subscription
subscribe nats subject queue cb = 
    let
        ssubject = makeSubject subject
        squeue = makeSubject `fmap` queue
        addToSubTable sid = atomicModifyIORef' (natsSubMap nats) $ \submap ->
                (Map.insert sid (NatsSubscription{subSubject=ssubject, subQueue=squeue, subCallback=cb, subSid=sid}) submap, ()) 
        removeFromSubTable sid = atomicModifyIORef' (natsSubMap nats) $ \submap ->
                (Map.delete sid submap, ()) 
    in do
        mvar <- newEmptyMVar :: IO (MVar (Maybe T.Text))
        sid <- newNatsSid nats
        bracketOnError
            (addToSubTable sid)
            (const $ removeFromSubTable sid)
            (\_ -> do
                sendMessage nats True (NatsClntSubscribe ssubject sid squeue) 
                    $ Just (putMVar mvar) -- Just put a result of operation to mvar
                merr <- takeMVar mvar
                case merr of
                    Just err -> throwIO $ NatsException $ T.unpack err
                    Nothing -> return $ sid
            )

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
            

-- | Synchronous request/response communication
request :: Nats 
    -> String             -- ^ Subject
    -> BL.ByteString      -- ^ Request
    -> IO BL.ByteString   -- ^ Response
request nats subject body = do
    mvar <- newEmptyMVar :: IO (MVar (Either String BL.ByteString))
    inbox <- newInbox
    bracket
            (subscribe nats inbox Nothing $ \_ _ response _ -> do
                _ <- tryPutMVar mvar (Right response)
                return ()
            ) 
            (\sid -> unsubscribe nats sid)
            (\_ -> do
                sendMessage nats True (NatsClntPublish (makeSubject subject) (Just $ makeSubject inbox) body) $ Just $ \merr -> do
                    case merr of
                         Nothing -> return ()
                         Just err -> tryPutMVar mvar (Left $ T.unpack err) >> return ()
                result <- takeMVar mvar
                case result of
                     Left err -> throwIO $ NatsException err
                     Right res -> return $ res
            )

-- | Publish a message
publish :: Nats 
    -> String -- ^ Subject
    -> BL.ByteString -- ^ Data
    -> IO ()
publish nats subject body = do
    -- Ignore errors - messages can get lost
    sendMessage nats False (NatsClntPublish (makeSubject subject) Nothing body) Nothing
        `catches` [ Handler (\(_ :: IOException) -> return ()),
                    Handler (\(_ :: NatsException) -> return ()) ]
    
-- | Disconnect from a NATS server
disconnect :: Nats -> IO ()
disconnect nats = do
    threadid <- readMVar (natsThreadId nats)
    killThread threadid
