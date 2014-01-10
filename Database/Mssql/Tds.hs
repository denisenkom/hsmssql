--module Main where
module Database.Mssql.Tds where

import Database.HDBC
import Database.HDBC.Types
import qualified Network.Socket as Sock
import Data.Bits
import Data.Sequence((|>))
import qualified Data.Sequence as Seq
import Data.Word
import Network.Socket.ByteString
import qualified Data.Binary.Get as LG
import Data.Binary.Strict.Get
import Data.Binary.Put
import qualified Data.Encoding as E
import Data.Encoding.UTF16
--import Data.Text.Encoding
--import qualified Data.Text as T
import qualified Data.Map as Map
import qualified Codec.Binary.UTF8.String as UTF8
import Data.List.Split(splitOn)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as B
import System.IO
import Data.Maybe
import Control.Monad

foreign import ccall unsafe "htons" htons :: Word16 -> Word16

data MacAddress = MacAddress Word8 Word8 Word8 Word8 Word8 Word8

data Token = TokError {number :: Word32,
                       state :: Word8,
                       cls :: Word8,
                       message :: String,
                       srvname :: String,
                       procname :: String,
                       lineno :: Word32}
           | TokLoginAck {iface :: Word8,
                          tdsver :: Word32,
                          progname :: String,
                          progver :: Word32}
           | TokEnvChange [EnvChange]
           | TokDone Word16 Word16 Word64
           | TokZero
           | TokColMetaDataEmpty
           | TokColMetaData [ColMetaData] [Token]
           | TokRow [Int]
     deriving(Eq, Show)

isTokLoginAck (TokLoginAck _ _ _ _) = True
isTokLoginAck _ = False

isTokError (TokError _ _ _ _ _ _ _) = True
isTokError _ = False

isTokMetaData (TokColMetaData _ _) = True
isTokMetaData TokColMetaDataEmpty = True
isTokMetaData _ = False

data EnvChange = PacketSize Int Int
     deriving(Eq, Show)

data TypeInfo = TypeInfo Word8
     deriving(Eq, Show)

data ColMetaData = ColMetaData Word32 Word16 TypeInfo String
     deriving(Eq, Show)

parseInstancesImpl :: Get ([String])
parseInstancesImpl = do
    pref <- getWord8
    _ <- getWord16be
    if pref == 5
        then do
            rem <- remaining
            str <- getByteString rem
            let tokens = splitOn ";" $ (UTF8.decode . BS.unpack) str
            return tokens
        else return []

parseInstances :: BS.ByteString -> Either String [Map.Map String String]
parseInstances s = 
    let res = runGet parseInstancesImpl s
    in case res of
        (Right tokens, _) -> Right $ tokensToDict tokens
        (Left err, _) -> Left err

tokensToDict tokens = tokensToDictImpl [] Map.empty tokens

tokensToDictImpl :: [Map.Map String String] -> Map.Map String String -> [String] -> [Map.Map String String]
tokensToDictImpl l m [] = l
tokensToDictImpl l m ("":"":[]) = (m:l)
tokensToDictImpl l m ("":xs) = tokensToDictImpl (m:l) Map.empty xs
tokensToDictImpl l m (k:v:xs) = tokensToDictImpl l (Map.insert k v m) xs

isInst name m = case Map.lookup "InstanceName" m of Just n -> n == name
                                                    Nothing -> False


queryInstances :: String -> IO [Map.Map String String]
queryInstances hoststr = do
    s <- Sock.socket Sock.AF_INET Sock.Datagram 0
    host <- Sock.inet_addr hoststr
    let addr = Sock.SockAddrInet (Sock.PortNum (htons 1434)) host
    sent <- Sock.sendTo s "\x3" addr
    res <- Network.Socket.ByteString.recv s (16 * 1024 - 1)
    let parse_res = parseInstances res
    case parse_res of
        Right instances -> return instances
        Left err -> do
            print err
            return []

packSQLBatch = 1
packLogin7 = 16
packPrelogin = 18

serializePacket :: Word8 -> B.ByteString -> Put
serializePacket packettype packet = do
    putWord8 packettype
    putWord8 1  -- final packet
    putWord16be $ 8 + fromIntegral (B.length packet)
    putWord16be 0 -- spid
    putWord8 0 -- packet no
    putWord8 0 -- padding
    putLazyByteString packet
    flush

preparePreLoginImpl :: Int -> [(Word8, B.ByteString)] -> [(Word8, B.ByteString, Int)]
preparePreLoginImpl _ [] = []
preparePreLoginImpl offset [(k, v)] = [(k, v, offset)]
preparePreLoginImpl offset ((k, v):xs) = 
    let offset' = (offset + fromIntegral (B.length v))
    in (k, v, offset):(preparePreLoginImpl offset' xs)

preloginVersion = 0 :: Word8
preloginEncryption = 1 :: Word8
preloginInstOpt = 2 :: Word8
preloginThreadId = 3 :: Word8
preloginMars = 4 :: Word8
preloginTraceId = 5 :: Word8
preloginTerminator = 0xff :: Word8

serializePreLogin :: Map.Map Word8  B.ByteString -> Put
serializePreLogin fields = do
    let fieldsWithOffsets = preparePreLoginImpl ((Map.size fields) * 5 + 1) (Map.assocs fields)
    forM fieldsWithOffsets (\(k, v, offset) -> do
        putWord8 k
        putWord16be $ fromIntegral offset
        putWord16be (fromIntegral (B.length v) :: Word16))
    putWord8 preloginTerminator
    forM (Map.elems fields) (\v -> do
        putLazyByteString v)
    return ()


sendPreLogin :: Handle -> Map.Map Word8 B.ByteString -> IO ()
sendPreLogin s prelogin =
    do let preloginbuf = runPut $ serializePreLogin prelogin
       B.hPutStr s $ runPut $ serializePacket packPrelogin preloginbuf


recvPreLogin :: Handle -> IO (Seq.Seq (Word8, Word16, Word16))
recvPreLogin s =
    do (packettype, _, preloginresppacket) <- getPacket s
       return $ LG.runGet getPreLoginHead preloginresppacket


getPacket :: Handle -> IO (Word8, Bool, B.ByteString)
getPacket s = do
    let decode = do
            packtype <- LG.getWord8
            isfinal <- LG.getWord8
            size <- LG.getWord16be
            LG.getWord16be  -- spid
            LG.getWord8  -- packet no
            LG.getWord8  -- padding
            return (packtype, isfinal /= 0, size)

    hdrbuf <- B.hGet s 8
    let (packtype, isfinal, size) = LG.runGet decode hdrbuf
    content <- B.hGet s ((fromIntegral size) - 8)
    return (packtype, isfinal, content)

getPreLoginHeadImpl :: Seq.Seq (Word8, Word16, Word16) -> LG.Get (Seq.Seq (Word8, Word16, Word16))
getPreLoginHeadImpl fields = do
    rectype <- LG.getWord8
    if rectype == preloginTerminator
        then return fields
        else do
            offset <- LG.getWord16be
            size <- LG.getWord16be
            getPreLoginHeadImpl $ fields |> (rectype, offset, size)

getPreLoginHead :: LG.Get (Seq.Seq (Word8, Word16, Word16))
getPreLoginHead = do
    getPreLoginHeadImpl Seq.empty

encodeUcs2 :: String -> B.ByteString
encodeUcs2 s = E.encodeLazyByteString UTF16LE s


manglePasswordByte :: Word8 -> Word8
manglePasswordByte ch = ((ch `shift` (-4)) .&. 0xff .|. (ch `shift` 4)) `xor` 0xA5

manglePassword :: String -> B.ByteString
manglePassword = (B.map manglePasswordByte) . encodeUcs2

verTDS74 = 0x74000004 :: Word32

serializeLogin :: (Word32, Word32, Word32, Word32, Word32,
                  Word8, Word8, Word8, Word8, Word32,
                  Word32, String, String, String, String,
                  String, B.ByteString, String, String, String,
                  MacAddress, B.ByteString, String,
                  String) -> Put
serializeLogin (tdsver, packsize, clientver, pid, connid, optflags1, optflags2,
               typeflags, optflags3, clienttz, clientlcid, hostname,
               username, password, appname, servername, extensionbuf, ctlintname,
               language, database, clientid, sspibuf, atchdbfile, changepwd) = do

    let hostnamebuf = encodeUcs2 hostname
        usernamebuf = encodeUcs2 username
        passwordbuf = manglePassword password
        appnamebuf = encodeUcs2 appname
        servernamebuf = encodeUcs2 servername
        ctlintnamebuf = encodeUcs2 ctlintname
        languagebuf = encodeUcs2 language
        databasebuf = encodeUcs2 database
        atchdbfilebuf = encodeUcs2 atchdbfile
        changepwdbuf = encodeUcs2 changepwd
        hostnameoff = 4+4+4+4+4+4+4+4+4+4+4+4+4+4+4+4+4+4+6+4+4+4+4 :: Word16
        len = fromIntegral . B.length
        off prevoff prevbuf = prevoff + (len prevbuf)
        usernameoff = off hostnameoff hostnamebuf
        passwordoff = off usernameoff usernamebuf
        appnameoff = off passwordoff passwordbuf
        servernameoff = off appnameoff appnamebuf
        extensionoff = off servernameoff servernamebuf
        ctlintnameoff = off extensionoff extensionbuf
        languageoff = off ctlintnameoff ctlintnamebuf
        databaseoff = off languageoff languagebuf
        sspioff = off databaseoff databasebuf
        atchdbfileoff = off sspioff sspibuf
        changepwdoff = off atchdbfileoff atchdbfilebuf
        putSOffLen off buf = do
            putWord16le off
            putWord16le $ (len buf) `quot` 2
        putOffLen off buf = do
            putWord16le off
            putWord16le $ len buf
        putMacAddr (MacAddress b0 b1 b2 b3 b4 b5) = do
            putWord8 b0
            putWord8 b1
            putWord8 b2
            putWord8 b3
            putWord8 b4
            putWord8 b5
        putData = do
            putWord32le tdsver
            putWord32le packsize
            putWord32le clientver
            putWord32le pid
            putWord32le connid
            putWord8 optflags1
            putWord8 optflags2
            putWord8 typeflags
            putWord8 optflags3
            putWord32le clienttz
            putWord32le clientlcid
            putSOffLen hostnameoff hostnamebuf
            putSOffLen usernameoff usernamebuf
            putSOffLen passwordoff passwordbuf
            putSOffLen appnameoff appnamebuf
            putSOffLen servernameoff servernamebuf
            putOffLen extensionoff extensionbuf
            putSOffLen ctlintnameoff ctlintnamebuf
            putSOffLen languageoff languagebuf
            putSOffLen databaseoff databasebuf
            putMacAddr clientid
            putOffLen sspioff sspibuf
            putSOffLen atchdbfileoff atchdbfilebuf
            putSOffLen changepwdoff changepwdbuf
            putWord32le $ fromIntegral (B.length sspibuf)
            -- header done, putting variable part
            putLazyByteString hostnamebuf
            putLazyByteString usernamebuf
            putLazyByteString passwordbuf
            putLazyByteString appnamebuf
            putLazyByteString servernamebuf
            putLazyByteString extensionbuf
            putLazyByteString ctlintnamebuf
            putLazyByteString languagebuf
            putLazyByteString databasebuf
            putLazyByteString sspibuf
            putLazyByteString atchdbfilebuf
            putLazyByteString changepwdbuf

        databuf = runPut putData

    putWord32le $ fromIntegral (B.length databuf) + 4
    putLazyByteString databuf


sendLogin :: Handle -> (Word32, Word32, Word32, Word32, Word32,
                  Word8, Word8, Word8, Word8, Word32,
                  Word32, String, String, String, String,
                  String, B.ByteString, String, String, String,
                  MacAddress, B.ByteString, String,
                  String) -> IO ()
sendLogin s login =
    do let loginbuf = runPut $ serializeLogin login
       B.hPutStr s $ runPut (serializePacket packLogin7 loginbuf)


recvTokens s =
    do (loginresptype, _, loginrespbuf) <- getPacket s
       let tokens = LG.runGet parseTokens loginrespbuf
           errors = filter isTokError tokens
       return tokens


tokenError = 170
tokenLoginAck = 173

getUsVarChar :: LG.Get String
getUsVarChar = do
    len <- LG.getWord16le
    buf <- LG.getLazyByteString ((fromIntegral len) * 2)
    return $ E.decodeLazyByteString UTF16LE buf

getBVarChar :: LG.Get String
getBVarChar = do
    len <- LG.getWord8
    buf <- LG.getLazyByteString ((fromIntegral len) * 2)
    return $ E.decodeLazyByteString UTF16LE buf

parseTypeInfo :: LG.Get TypeInfo
parseTypeInfo = do
    typeid <- LG.getWord8
    return $ TypeInfo typeid

parseRowCol :: ColMetaData -> LG.Get Int
parseRowCol col = do
    val <- LG.getWord32le
    return $ fromIntegral val

parseRowHelper :: [ColMetaData] -> LG.Get [Int]
parseRowHelper [] = return []
parseRowHelper (col:xs) = do
    val <- parseRowCol col
    vals <- parseRowHelper xs
    return $ val:vals

parseRowM :: [ColMetaData] -> LG.Get (Maybe Token)
parseRowM cols = do
    tok <- LG.getWord8
    case tok of
        209 -> do
            vals <- parseRowHelper cols
            return $ Just (TokRow vals)
        otherwise -> return Nothing

parseRows :: [ColMetaData] -> LG.Get [Token]
parseRows cols = do
    rowm <- LG.lookAheadM $ parseRowM cols
    case rowm of
        Just row -> do
            rows <- parseRows cols
            return (row:rows)
        Nothing -> return []

parseColMetaData72 :: LG.Get Token
parseColMetaData72 = do
    let parseCol = do
            usertype <- LG.getWord32le
            flags <- LG.getWord16le
            ti <- parseTypeInfo
            name <- getBVarChar
            return $ ColMetaData usertype flags ti name
        parseMeta 0xffff = return TokColMetaDataEmpty
        parseMeta cnt = do
            cols <- parseCols cnt
            rows <- parseRows cols
            return $ TokColMetaData cols rows
        parseCols 0 = return []
        parseCols cnt = do
            col <- parseCol
            cols <- parseCols $ cnt - 1
            return (col:cols)

    cnt <- LG.getWord16le
    parseMeta cnt


parseLoginAck = do
    LG.getWord16le
    iface <- LG.getWord8
    tdsver <- LG.getWord32be
    progname <- getBVarChar
    progver <- LG.getWord32be
    return $ TokLoginAck iface tdsver progname progver

parseError = do
    LG.getWord16le
    number <- LG.getWord32le
    state <- LG.getWord8
    cls <- LG.getWord8
    message <- getUsVarChar
    srvname <- getBVarChar
    procname <- getBVarChar
    lineno <- LG.getWord32le
    return $ TokError number state cls message srvname procname lineno


parseEnvChange = do
    let parseEnvChangeRec = do
            envtype <- LG.getWord8
            case envtype of
                4 -> do
                    curval <- getBVarChar
                    prevval <- getBVarChar
                    return $ PacketSize (read curval) (read prevval)

    size <- LG.getWord16le
    envchgrec <- parseEnvChangeRec
    return $ TokEnvChange [envchgrec]

parseDone = do
    status <- LG.getWord16le
    curcmd <- LG.getWord16le
    rowcount <- LG.getWord64le
    return $ TokDone status curcmd rowcount

parseToken :: LG.Get Token
parseToken = do
    tok <- LG.getWord8
    case tok of
        0 -> do
            return TokZero
        129 -> do
            parseColMetaData72
        170 -> do
            parseError
        173 -> do
            parseLoginAck
        227 -> do
            parseEnvChange
        253 -> do
            parseDone
        _ -> do
            fail $ "unknown token type " ++ (show tok)

parseTokens :: LG.Get [Token]
parseTokens = do
    empty <- LG.isEmpty
    if empty
        then return []
        else do
            token <- parseToken
            tokens <- parseTokens
            return (token:tokens)


getPort host inst = do
    if inst /= ""
        then do
            instances <- queryInstances host
            let ports = catMaybes [Map.lookup "tcp" i| i <- instances, isInst inst i]
            let port = (read (head ports) :: Word16)
            return port
        else return 1433


dataStmHdrQueryNotif = 1  -- query notifications
dataStmHdrTransDescr = 2  -- MARS transaction descriptor (required)
dataStmHdrTraceActivity = 3

data DataStmHeader = DataStmHeader Word16 B.ByteString
            | DataStmTransDescrHdr Word64 Word32


putDataStmHeaders headers = do
    let putHeader (DataStmHeader htype dat) = do
            putWord16le htype
            putLazyByteString dat
        putHeader (DataStmTransDescrHdr transDescr reqCnt) = do
            putWord16le dataStmHdrTransDescr
            putWord64le transDescr
            putWord32le reqCnt
        putHeaders [] = return ()
        putHeaders (header:xs) = do
            let hdrbuf = runPut $ putHeader header
                hdrsize = fromIntegral (B.length hdrbuf) + 4
            putWord32le hdrsize
            putLazyByteString hdrbuf
            putHeaders xs
        headersbuf = runPut $ putHeaders headers
        headerssize = fromIntegral (B.length headersbuf) + 4

    putWord32le headerssize
    putLazyByteString headersbuf




sendSqlBatch72 :: Handle -> [DataStmHeader] -> String -> IO ()
sendSqlBatch72 s headers query = do
    let putContent = do
            putDataStmHeaders headers
            putLazyByteString $ encodeUcs2 query
        packetbuf = runPut putContent
    B.hPutStr s $ runPut $ serializePacket packSQLBatch packetbuf


exec :: Handle -> String -> IO [Token]
exec s query = do
    let headers = [DataStmTransDescrHdr 0 1]
    sendSqlBatch72 s headers query
    (resptype, _, respbuf) <- getPacket s
    let tokens = LG.runGet parseTokens respbuf
        errors = filter isTokError tokens
    return tokens